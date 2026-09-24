#include "TcpSendChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/Core/TcpSocket.hpp>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>

namespace Foundation::NBIO
{
class SendAwaiter
{
  public:
    SendAwaiter(TcpSendChannel &channel, std::span<const char> buffer) : channel_(channel), buffer_(buffer)
    {
    }

    SendAwaiter(const SendAwaiter &) = delete;
    SendAwaiter &operator=(const SendAwaiter &) = delete;

    // No cancellation hook. The parked Coroutine keeps this frame's control block
    // alive, and the channel owns the waiter until it answers it, so a stale
    // registration is impossible rather than detected.

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
    {
        transmission_.status = Core::OperationStatus::kPending;
        transmission_.bytes = 0;
        transmission_.error_code = {};
        // The transmission buffer is non-const only for C API compatibility: the
        // backend reads it, never writes through it.
        transmission_.buffer = std::span<char>(const_cast<char *>(buffer_.data()), buffer_.size());
        channel_.prepare(Async::Coroutine::from_handle(handle), &transmission_);
        channel_.arm();
    }

    Core::Transmission await_resume() noexcept
    {
        return transmission_;
    }

  private:
    TcpSendChannel &channel_;
    std::span<const char> buffer_;
    Core::Transmission transmission_{};
};

TcpSendChannel::TcpSendChannel(Foundation::Core::TcpSocket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kSend, socket.native_handle(), multiplexer, scheduler), socket_(socket)
{
    if (!socket_.is_valid())
    {
        throw std::logic_error("socket is invalid");
    }
    if (auto result = socket_.non_blocking(true); !result)
    {
        throw std::system_error(result.error(), "non_blocking failed");
    }
    // Registered on the first arm(): nothing is watched until a send queues.
}

TcpSendChannel::~TcpSendChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void TcpSendChannel::prepare(Foundation::Async::Coroutine waiter, Core::Transmission *transmission)
{
    waiters_.push_back(std::move(waiter));
    auto &payload = std::get<SendPayload>(payload_);
    payload.submit(transmission);
}

Payload &TcpSendChannel::submit()
{
    return payload_;
}

void TcpSendChannel::complete()
{
    auto &payload = std::get<SendPayload>(payload_);
    while (auto completion = payload.next_completion())
    {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.submit(std::move(waiter));
        payload.conclude();
    }
    if (payload.size() != 0)
    {
        arm();
    }
    else
    {
        disarm();
    }
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> TcpSendChannel::send(std::span<const char> buffer)
{
    auto result = co_await SendAwaiter{*this, buffer};
    if (result.status == Core::OperationStatus::kError)
    {
        co_return Core::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
} // namespace Foundation::NBIO
