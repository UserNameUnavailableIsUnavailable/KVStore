#include "TcpSendChannel.hpp"

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Net/TcpSocket.hpp>

#include <NBIO/Runtime/Runtime.hpp>
#include <span>
#include <stdexcept>
#include <utility>

namespace NBIO::Net {
class SendAwaiter {
   public:
    SendAwaiter(TcpSendChannel& channel, std::span<const char> buffer) : channel_(channel), buffer_(buffer) {}

    SendAwaiter(const SendAwaiter&) = delete;
    SendAwaiter& operator=(const SendAwaiter&) = delete;

    // No cancellation hook. The parked Coroutine keeps this frame's control block
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
        channel_.prepare(Async::Coroutine::from_handle(handle), &transmission_);
        channel_.arm();
    }

    Net::Transmission await_resume() noexcept { return transmission_; }

   private:
    TcpSendChannel& channel_;
    std::span<const char> buffer_;
    Net::Transmission transmission_{};
};

TcpSendChannel::TcpSendChannel(NBIO::Net::TcpConnector& connector, NBIO::Core::Multiplexer& multiplexer,
                               NBIO::Async::Scheduler& scheduler)
    : NBIO::Core::Channel<TcpSendChannel>(NBIO::Core::ChannelType::kSend, connector.native_handle(),
                                                multiplexer, scheduler),
      connector_(connector) {
    if (!connector.is_valid()) {
        throw std::logic_error("socket is invalid");
    }
    if (auto result = connector.non_blocking(true); !result) {
        throw std::system_error(result.error(), "non_blocking failed");
    }
    // Registered on the first arm(): nothing is watched until a send queues.
}

TcpSendChannel::~TcpSendChannel() noexcept { multiplexer_.delete_channel(this); }

void TcpSendChannel::prepare(NBIO::Async::Coroutine waiter, Net::Transmission* transmission) {
    waiters_.push_back(std::move(waiter));
    auto& payload = payload_;
    payload.submit(transmission);
}

TcpSendChannel::Payload& TcpSendChannel::submit() { return payload_; }

void TcpSendChannel::complete() {
    auto& payload = payload_;
    while (auto completion = payload.next_completion()) {
        auto waiter = std::move(waiters_.front());
        waiters_.pop_front();
        scheduler_.submit(std::move(waiter));
        payload.conclude();
    }
    if (payload.size() != 0) {
        arm();
    } else {
        disarm();
    }
}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> TcpSendChannel::send(
    std::span<const char> buffer) {
    auto result = co_await SendAwaiter{*this, buffer};
    if (result.status == OperationStatus::kError) {
        co_return Utility::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
}  // namespace NBIO::Net




