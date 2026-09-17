#include "SendChannel.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <optional>
#include <span>
#include <utility>

namespace Foundation::NBIO
{
namespace detail
{
class SendAwaiter
{
  public:
    SendAwaiter(SendChannel &channel, std::span<const char> &buffer) : channel_(channel), buffer_(buffer)
    {
    }

    SendAwaiter(const SendAwaiter &) = delete;
    SendAwaiter &operator=(const SendAwaiter &) = delete;

    // No cancellation hook. The parked CoroutineView keeps this frame's control
    // block alive, and the channel checks is_dead() before it touches the job,
    // so a stale registration is detected rather than prevented.

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        channel_.arm();
        channel_.park(Foundation::Async::Coroutine::from_handle(handle));
        SendJob job{.buffer = buffer_,
                    .result = {.status = Foundation::Core::SendStatus::kPending, .bytes_transferred = 0, .error_code = {}}};
        std::memcpy(&channel_.job(), &job, sizeof(SendJob));
    }

    Foundation::Core::SendResult await_resume() noexcept
    {
        const SendJob &job = channel_.job();
        assert(job.result.status != Foundation::Core::SendStatus::kPending);
        return job.result;
    }

  private:
    SendChannel &channel_;
    std::span<const char> buffer_;
};
} // namespace

SendChannel::SendChannel(Foundation::Core::Socket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kSend, socket.native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket_.is_valid())
    {
        throw std::logic_error("socket is invalid");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

SendChannel::~SendChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void SendChannel::handle_event()
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

    if (job_.result.status == Foundation::Core::SendStatus::kPending)
    {
        // Not finished (EAGAIN, or partially written): stay armed.
        arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}

Foundation::NBIO::Task<std::optional<std::size_t>> SendChannel::send(std::span<const char> buffer)
{
    auto result = co_await detail::SendAwaiter(*this, buffer);
    std::optional<std::size_t> ret{};
    if (result.status == Core::SendStatus::kError)
    {
        error_code_ = std::move(result.error_code);
    }
    else
    {
        ret = result.bytes_transferred;
    }
    co_return ret;
}
} // namespace Foundation::NBIO
