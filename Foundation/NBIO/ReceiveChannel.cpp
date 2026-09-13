#include "ReceiveChannel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <spdlog/spdlog.h>

#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <stdexcept>
#include <utility>

#include <Foundation/Async/Task.hpp>

namespace Foundation::NBIO
{
// The awaiter lives here: it touches the channel's job and waiter, so it
// operates on the concrete (simplex) channel type.
namespace detail
{
class ReceiveAwaiter
{
  public:
    ReceiveAwaiter(ReceiveChannel &channel, Foundation::Core::Buffer &buffer) : channel_(channel), buffer_(buffer)
    {
    }

    ReceiveAwaiter(const ReceiveAwaiter &) = delete;
    ReceiveAwaiter &operator=(const ReceiveAwaiter &) = delete;

    // No cancellation hook. The parked CoroutineView keeps this frame's control
    // block alive, and the channel checks is_dead() before it touches the job,
    // so a stale registration is detected rather than prevented.

    bool await_ready() const noexcept
    {
        return false;
    }

    // Templated on the concrete promise type: coroutine_handle has no
    // derived-to-base conversion, so the compiler-passed
    // coroutine_handle<promise_type> cannot bind to coroutine_handle<Promise>.
    // The base Promise is reached through a reference instead (references do
    // support derived-to-base).
    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        static_assert(std::is_base_of_v<Foundation::Async::Promise, PromiseType>,
                      "ReceiveAwaiter requires a promise derived from Foundation::Async::Promise");
        channel_.arm();
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        ReceiveJob job{.buffer = &buffer_,
                       .result = {.status = Foundation::Core::ReceiveStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        std::memcpy(&channel_.job(), &job, sizeof(ReceiveJob));
    }

    Foundation::Core::ReceiveResult await_resume() noexcept
    {
        const ReceiveJob &job = channel_.job();
        assert(job.result.status != Foundation::Core::ReceiveStatus::kPending);
        return job.result;
    }

  private:
    ReceiveChannel &channel_;
    ::Foundation::Core::Buffer &buffer_;
};
} // namespace detail
ReceiveChannel::ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kReceive, socket.native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

ReceiveChannel::~ReceiveChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void ReceiveChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    if (!waiter_) [[unlikely]]
    {
        waiter_ = {};
        return;
    }

    if (job_.result.status == Foundation::Core::ReceiveStatus::kPending)
    {
        // Not finished (e.g. EAGAIN): stay armed and keep the coroutine suspended.
        arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}

Foundation::NBIO::Task<Foundation::Core::ReceiveResult> ReceiveChannel::receive(Foundation::Core::Buffer &buffer)
{
    auto awaiter = detail::ReceiveAwaiter(*this, buffer);
    auto result = co_await std::move(awaiter);
    co_return std::move(result);
}
} // namespace Foundation::NBIO
