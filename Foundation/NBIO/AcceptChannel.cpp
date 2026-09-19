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
    // cannot come.
    for (PendingAccept &pending : pending_)
    {
        drop(pending);
    }
    pending_.clear();
    submitted_ = 0;
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

void AcceptChannel::prepare(Async::Coroutine waiter, Foundation::Core::AcceptResult &result)
{
    result = {.status = Foundation::Core::AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}};
    pending_.emplace_back(std::move(waiter), result);
    refresh_arming();
}

void AcceptChannel::refresh_arming() noexcept
{
    const bool can_hand_over = has_prepared();

    if (submits_immediately())
    {
        // Readiness is the submission: the wait goes over now, and the channel
        // stays armed while there is anything to wait for.
        if (can_hand_over)
        {
            count_prepared();
        }

        if (submitted_ == 0 && pending_.empty())
        {
            disarm();
            return;
        }
        arm();
        return;
    }

    // A completion backend runs the submission phase itself, so the channel is
    // armed exactly while it has something for that phase to hand over.
    if (can_hand_over)
    {
        arm();
        return;
    }
    disarm();
}

std::size_t AcceptChannel::count_prepared() noexcept
{
    if (submitted_ != 0 || pending_.empty())
    {
        // One accept is in front of the kernel at a time: a completion names the
        // channel, not the wait it belongs to, so the wait being accepted for has
        // to be the only one outstanding.
        return 0;
    }

    // The front is always a wait that has no answer yet -- an answered one is
    // retired as it is answered -- so this is where the kernel's peer address goes.
    pending_.front().result() = {.status = Foundation::Core::AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}};
    submitted_ = 1;
    return 1;
}

void AcceptChannel::wake_front() noexcept
{
    PendingAccept finished = std::move(pending_.front());
    pending_.pop_front();
    submitted_ = 0;
    if (finished.waiter())
    {
        scheduler_.submit(std::move(finished.waiter()));
    }
}

void AcceptChannel::commit_result(Foundation::Core::AcceptResult accepted) noexcept
{
    if (submitted_ == 0)
    {
        return;
    }

    pending_.front().result() = std::move(accepted);
    wake_front();
}

void AcceptChannel::complete_tasks(std::ptrdiff_t result) noexcept
{
    if (submitted_ == 0)
    {
        if (result >= 0)
        {
            // Nobody is waiting for this connection any more. It is still ours, so
            // it is closed rather than leaked.
            [[maybe_unused]] auto closed = Foundation::Core::Socket::Adopt(static_cast<std::uintptr_t>(result));
        }
        return;
    }

    Foundation::Core::AcceptResult &slot = pending_.front().result();
    if (result >= 0)
    {
        // The peer address was written in place when the operation was built, so
        // the socket and the status are all that is left to fill.
        slot.status = Foundation::Core::AcceptStatus::kDone;
        slot.socket = Foundation::Core::Socket::Adopt(static_cast<std::uintptr_t>(result));
        wake_front();
        return;
    }

    const int error = -static_cast<int>(result);
    if (error == EAGAIN || error == EWOULDBLOCK)
    {
        // Not ready is not an answer: the wait keeps its place at the front of the
        // queue and is handed to the kernel again.
        slot.status = Foundation::Core::AcceptStatus::kPending;
        submitted_ = 0;
        return;
    }

    slot.status = Foundation::Core::AcceptStatus::kError;
    slot.error_code = std::error_code(error, std::system_category());
    wake_front();
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
    // An answered wait was retired as it was answered, so all that is left is to
    // say what the backend owes next.
    refresh_arming();
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
