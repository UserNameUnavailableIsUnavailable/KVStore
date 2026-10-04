#include "TcpSendChannel.hpp"

#include <nbio/async/Coroutine.hpp>
#include <nbio/net/TcpSocket.hpp>

#include <nbio/runtime/Runtime.hpp>
#include <span>
#include <stdexcept>
#include <utility>

namespace nbio::net {
class SendAwaiter {
   public:
    SendAwaiter(TcpSendChannel& channel, std::span<const char> buffer) : channel_(channel), buffer_(buffer) {}

    SendAwaiter(const SendAwaiter&) = delete;
    SendAwaiter& operator=(const SendAwaiter&) = delete;

    // No cancellation hook. The Parked Coroutine keeps this frame's control block
    // alive, and the channel owns the waiter until it answers it, so a stale
    // registration is impossible rather than detected.

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept {
        transmission_.status = OperationStatus::kPending;
        transmission_.bytes = 0;
        transmission_.error_code = {};
        // The transmission buffer is non-const only for C API compatibility: the
        // backend reads it, never writes through it.
        transmission_.buffer = std::span<char>(const_cast<char*>(buffer_.data()), buffer_.size());
        channel_.Prepare(async::Coroutine::FromHandle(handle), &transmission_);
        channel_.Arm();
    }

    net::Transmission await_resume() noexcept { return transmission_; }

   private:
    TcpSendChannel& channel_;
    std::span<const char> buffer_;
    net::Transmission transmission_{};
};

TcpSendChannel::TcpSendChannel(nbio::net::TcpConnector& connector, nbio::core::Multiplexer& multiplexer,
                               nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<TcpSendChannel>(nbio::core::ChannelType::kSend, connector.native_handle(),
                                                multiplexer, scheduler),
      connector_(connector) {
    if (!connector.IsValid()) {
        throw std::logic_error("socket is invalid");
    }
    if (auto result = connector.NonBlocking(true); !result) {
        throw std::system_error(result.error(), "NonBlocking failed");
    }
    // Registered on the first arm(): nothing is watched until a send queues.
}

TcpSendChannel::~TcpSendChannel() noexcept { multiplexer_.DeleteChannel(this); }

void TcpSendChannel::Prepare(nbio::async::Coroutine waiter, net::Transmission* transmission) {
    waiters_.push_back(std::move(waiter));
    auto& payload = payload_;
    payload.submit(transmission);
}

TcpSendChannel::Payload& TcpSendChannel::Submit() { return payload_; }

void TcpSendChannel::Complete() {
    auto& payload = payload_;
    while (auto completion = payload.next_completion()) {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.Submit(std::move(waiter));
        payload.conclude();
    }
    if (payload.size() != 0) {
        Arm();
    } else {
        Disarm();
    }
}

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> TcpSendChannel::Send(
    std::span<const char> buffer) {
    auto result = co_await SendAwaiter{*this, buffer};
    if (result.status == OperationStatus::kError) {
        co_return utility::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
}  // namespace nbio::net




