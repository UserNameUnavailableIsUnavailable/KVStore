#include "AcceptChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <cerrno>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::NBIO
{
AcceptChannel::AcceptChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Channel(Foundation::NBIO::ChannelType::kAccept, static_cast<std::uintptr_t>(socket.native_handle()), multiplexer, scheduler), listener_(std::move(socket))
{
    if (!listener_.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    listener_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

AcceptChannel::~AcceptChannel() noexcept
{
    multiplexer_.delete_channel(this);

    // Whatever is still queued has nowhere to land now: every waiting frame gets an
    // outcome and a wake-up rather than being left parked for a completion that
    // cannot come. A wait that already has its answer keeps it -- only the wake-up
    // is still owed to it.
    for (PendingAccept &pending : prepared_waits_)
    {
        drop(pending);
    }
    for (PendingAccept &pending : submitted_waits_)
    {
        drop(pending);
    }
    for (PendingAccept &done : completed_waits_)
    {
        if (done.waiter())
        {
            scheduler_.submit(std::move(done.waiter()));
        }
    }
    prepared_waits_.clear();
    submitted_waits_.clear();
    completed_waits_.clear();
}

class AcceptAwaiter
{
  public:
    AcceptAwaiter(AcceptChannel &channel) : channel_(channel)
    {
    }
    AcceptAwaiter(const AcceptAwaiter &) = delete;
    AcceptAwaiter &operator=(const AcceptAwaiter &) = delete;

    ~AcceptAwaiter() noexcept = default;

    bool await_ready() const noexcept
    {
        return false;
    }
    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        channel_.prepare(Async::Coroutine::from_handle(handle), result_);
    }

    Foundation::Core::AcceptResult await_resume() noexcept
    {
        return std::move(result_);
    }

  private:
    AcceptChannel &channel_;
    Foundation::Core::AcceptResult result_{};
};

// The accept body, as a plain coroutine whose channel is an ordinary parameter
// (see the note on AcceptChannel::accept).
static Foundation::NBIO::Task<std::optional<std::pair<Core::Socket, Core::Address>>> AcceptOn(AcceptChannel &channel)
{
    auto result = co_await AcceptAwaiter(channel);
    std::optional<std::pair<Core::Socket, Core::Address>> ret{};
    if (result.status != Core::AcceptStatus::kError)
    {
        ret = std::make_pair(std::move(result.socket), std::move(result.address));
    }
    co_return ret;
}

PendingAccept *AcceptChannel::submit_jobs()
{
    if (!submitted_waits_.empty())
    {
        // One accept is in front of the kernel at a time: an answer names the
        // channel, not the wait it belongs to, so the wait being accepted for has to
        // be the only one outstanding. The backend asks again once it is answered.
        return nullptr;
    }
    if (prepared_waits_.empty())
    {
        return nullptr; // nobody is waiting
    }

    submitted_waits_.push_back(std::move(prepared_waits_.front()));
    prepared_waits_.pop_front();

    // The front is always a wait that has no answer yet -- an answered one is
    // retired as it is answered -- so this is where the kernel's peer address goes.
    PendingAccept &waiting = submitted_waits_.front();
    waiting.result() = {.status = Foundation::Core::AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}};
    return &waiting;
}

void AcceptChannel::advance_job(std::ptrdiff_t result) noexcept
{
    if (submitted_waits_.empty())
    {
        if (result >= 0)
        {
            // Nobody is waiting for this connection any more. It is still ours, so
            // it is closed rather than leaked.
            [[maybe_unused]] auto closed = Foundation::Core::Socket::Adopt(static_cast<std::uintptr_t>(result));
        }
        return;
    }

    if (result >= 0)
    {
        // The peer address was written in place when the operation was built, so
        // the socket is all that is left to fill.
        Foundation::Core::AcceptResult accepted = std::move(submitted_waits_.front().result());
        accepted.status = Foundation::Core::AcceptStatus::kDone;
        accepted.socket = Foundation::Core::Socket::Adopt(static_cast<std::uintptr_t>(result));
        retire_front(std::move(accepted));
        return;
    }

    const int error = -static_cast<int>(result);
    if (error == EAGAIN || error == EWOULDBLOCK)
    {
        // Not ready is not an answer: the wait keeps its place at the front of the
        // queue and is handed to the backend again.
        submitted_waits_.front().result().status = Foundation::Core::AcceptStatus::kPending;
        return;
    }

    retire_front({.status = Foundation::Core::AcceptStatus::kError,
                  .socket = {},
                  .address = {},
                  .error_code = std::error_code(error, std::system_category())});
}

void AcceptChannel::advance_job(Foundation::Core::AcceptResult accepted) noexcept
{
    if (submitted_waits_.empty())
    {
        // Nobody is waiting any more, so the connection the backend took is closed
        // by the result going out of scope.
        return;
    }

    if (accepted.status == Foundation::Core::AcceptStatus::kPending)
    {
        // Nothing to take right now: the wait keeps its place.
        return;
    }

    retire_front(std::move(accepted));
}

void AcceptChannel::complete_jobs() noexcept
{
    // The operation is over. A wait it did not answer goes back to the front of the
    // queue, so the next operation takes it.
    if (!submitted_waits_.empty())
    {
        prepared_waits_.push_front(std::move(submitted_waits_.front()));
        submitted_waits_.clear();
    }
}

void AcceptChannel::retire_front(Foundation::Core::AcceptResult result) noexcept
{
    PendingAccept finished = std::move(submitted_waits_.front());
    submitted_waits_.pop_front();
    finished.result() = std::move(result);
    completed_waits_.push_back(std::move(finished));
}

void AcceptChannel::prepare(Async::Coroutine waiter, Foundation::Core::AcceptResult &result)
{
    result = {.status = Foundation::Core::AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}};
    prepared_waits_.emplace_back(std::move(waiter), result);

    // Armed from the moment there is something to wait for: an armed channel is one
    // the backend looks at, and it stays armed until the queue runs dry.
    arm();
}

void AcceptChannel::drop(PendingAccept &pending) noexcept
{
    pending.result() = {.status = Foundation::Core::AcceptStatus::kError,
                        .socket = {},
                        .address = {},
                        .error_code = std::make_error_code(std::errc::operation_canceled)};
    if (pending.waiter())
    {
        scheduler_.submit(std::move(pending.waiter()));
    }
}

void AcceptChannel::handle_completion()
{
    // Every wait that has an answer wakes here. This runs once the batch's
    // completions have all been advanced, so a resumed accept cannot queue itself
    // behind a wait that is still being answered.
    for (PendingAccept &waiting : completed_waits_)
    {
        if (waiting.waiter())
        {
            scheduler_.submit(std::move(waiting.waiter()));
        }
    }
    completed_waits_.clear();

    // Armed while there is anything left to wait for, disarmed otherwise: the
    // backend submits for an armed channel and leaves a disarmed one alone.
    if (prepared_waits_.empty())
    {
        disarm();
    }
    else
    {
        arm();
    }
}

Foundation::NBIO::Task<std::optional<std::pair<Core::Socket, Core::Address>>> AcceptChannel::accept()
{
    // Deliberately not a member coroutine: the implicit object parameter of a
    // member coroutine is laid out by the compiler in the same frame slot the
    // promise uses, so `*this` inside the body came back as the inherited
    // control block instead of the channel. Taking the channel as an ordinary
    // parameter keeps it in the parameter area.
    return AcceptOn(*this);
}
} // namespace Foundation::NBIO
