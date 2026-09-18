#include "AcceptChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <optional>
#include <stdexcept>
#include <utility>

#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::NBIO
{
AcceptChannel::AcceptChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kListen, socket.native_handle(), multiplexer, scheduler),
      socket_(std::move(socket))
{
    if (!socket_.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    socket_.set_non_blocking(true);
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
        fail(pending);
    }
    pending_.clear();
}

namespace
{
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
    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        channel_.submit(result_, Async::Coroutine::from_handle(handle));
    }

    Foundation::Core::AcceptResult await_resume() noexcept
    {
        return std::move(result_);
    }

  private:
    AcceptChannel &channel_;
    // Where this wait's outcome lands. It is this frame's own storage, and the
    // kernel is handed a pointer into it while the accept is in flight.
    Foundation::Core::AcceptResult result_{};
};

// The accept body, as a plain coroutine whose channel is an ordinary parameter
// (see the note on AcceptChannel::accept).
Foundation::NBIO::Task<std::optional<std::pair<Core::Socket, Core::Address>>> AcceptOn(AcceptChannel &channel)
{
    auto result = co_await AcceptAwaiter(channel);
    std::optional<std::pair<Core::Socket, Core::Address>> ret{};
    if (result.status != Core::AcceptStatus::kError)
    {
        ret = std::make_pair(std::move(result.socket), std::move(result.address));
    }
    co_return ret;
}
} // namespace

void AcceptChannel::submit(Foundation::Core::AcceptResult &result, Async::Coroutine waiter)
{
    const bool was_empty = pending_.empty();
    pending_.push_back(PendingAccept{.result = &result, .waiter = std::move(waiter)});
    if (was_empty)
    {
        arm_next();
    }
}

void AcceptChannel::arm_next()
{
    if (pending_.empty())
    {
        return;
    }

    // One wait is in front of the kernel at a time: a completion says a connection
    // arrived, and the peer address went into the result of the wait that was
    // submitted, so that wait is the one it belongs to.
    Foundation::Core::AcceptResult &next = *pending_.front().result;
    next = {.status = Foundation::Core::AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}};
    arm();
}

void AcceptChannel::accepted(int result) noexcept
{
    if (pending_.empty())
    {
        return;
    }

    Foundation::Core::AcceptResult &slot = *pending_.front().result;
    if (result >= 0)
    {
        // The accepted socket's address was written in place when the accept was
        // submitted, so the status and the socket are all that is left to fill.
        slot.status = Foundation::Core::AcceptStatus::kDone;
        slot.socket = Foundation::Core::Socket::Adopt(result);
        return;
    }

    if (result == -EAGAIN || result == -EWOULDBLOCK)
    {
        slot.status = Foundation::Core::AcceptStatus::kPending;
        return;
    }

    slot.status = Foundation::Core::AcceptStatus::kError;
    slot.error_code = std::error_code(-result, std::system_category());
}

void AcceptChannel::flush() noexcept
{
    // Taking connections is the one operation here that can be repeated: a
    // readiness event says "at least one connection", so take them while they last
    // and while somebody is waiting for one.
    while (!pending_.empty())
    {
        Foundation::Core::AcceptResult accepted_now = socket_.accept();
        if (accepted_now.status == Foundation::Core::AcceptStatus::kPending)
        {
            // Nothing more is there: the waits that are left keep their places.
            break;
        }

        PendingAccept finished = std::move(pending_.front());
        pending_.pop_front();
        if (finished.result != nullptr)
        {
            *finished.result = std::move(accepted_now);
        }
        if (finished.waiter)
        {
            scheduler_.submit(std::move(finished.waiter));
        }
    }
}

void AcceptChannel::fail(PendingAccept &pending) noexcept
{
    if (pending.result != nullptr)
    {
        *pending.result = {.status = Foundation::Core::AcceptStatus::kError,
                           .socket = {},
                           .address = {},
                           .error_code = std::make_error_code(std::errc::operation_canceled)};
    }
    if (pending.waiter)
    {
        scheduler_.submit(std::move(pending.waiter));
    }
}

void AcceptChannel::handle_event()
{
    // handle the triggered event
    if (handler_ != nullptr) [[likely]]
    {
        handler_(this);
    }

    // Wake every wait that has an answer: a readiness backend can have taken
    // several connections in the one event.
    while (!pending_.empty() && pending_.front().result != nullptr &&
           pending_.front().result->status != Foundation::Core::AcceptStatus::kPending)
    {
        PendingAccept finished = std::move(pending_.front());
        pending_.pop_front();
        if (finished.waiter)
        {
            scheduler_.submit(std::move(finished.waiter));
        }
    }

    if (!pending_.empty())
    {
        // Whoever is at the front of the queue waits for the next connection.
        arm_next();
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
