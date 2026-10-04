#include "TcpReceiveChannel.hpp"

#include <nbio/async/Coroutine.hpp>
#include <nbio/net/TcpConnector.hpp>

#include <nbio/runtime/Runtime.hpp>
#include <span>
#include <stdexcept>
#include <utility>

namespace nbio::net {
class ReceiveAwaiter {
   public:
    ReceiveAwaiter(TcpReceiveChannel& channel, std::span<char> buffer) : channel_(channel), buffer_(buffer) {}

    ReceiveAwaiter(const ReceiveAwaiter&) = delete;
    ReceiveAwaiter& operator=(const ReceiveAwaiter&) = delete;

    // No cancellation hook. The Parked Coroutine keeps this frame's control block
    // alive, and the channel owns the waiter until it answers it, so a stale
    // registration is impossible rather than detected.

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept {
        transmission_.status = OperationStatus::kPending;
        transmission_.bytes = 0;
        transmission_.error_code = {};
        transmission_.buffer = buffer_;
        channel_.Prepare(async::Coroutine::FromHandle(handle), &transmission_);
        channel_.Arm();
    }

    net::Transmission await_resume() noexcept { return transmission_; }

   private:
    TcpReceiveChannel& channel_;
    std::span<char> buffer_;
    net::Transmission transmission_{};
};

TcpReceiveChannel::TcpReceiveChannel(nbio::net::TcpConnector& connector,
                                     nbio::core::Multiplexer& multiplexer,
                                     nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<TcpReceiveChannel>(nbio::core::ChannelType::kReceive,
                                                   static_cast<std::uintptr_t>(connector.native_handle()), multiplexer,
                                                   scheduler),
      connector_(connector) {
    if (!connector_.IsValid()) {
        throw std::logic_error("invalid connector");
    }
    // A receive that blocks in the call would hold the whole engine until the peer
    // sent something, so the connection is non-blocking from here.
    if (auto result = connector_.NonBlocking(true); !result) {
        throw std::system_error(result.error(), "NonBlocking failed");
    }
    // Registered on the first arm(): nothing is watched until a receive queues.
}

TcpReceiveChannel::~TcpReceiveChannel() noexcept { multiplexer_.DeleteChannel(this); }

void TcpReceiveChannel::Prepare(async::Coroutine waiter, net::Transmission* transmission) {
    waiters_.push_back(std::move(waiter));
    auto& payload = payload_;
    payload.submit(transmission);
}

TcpReceiveChannel::Payload& TcpReceiveChannel::Submit() { return payload_; }

void TcpReceiveChannel::Complete() {
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

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> TcpReceiveChannel::Receive(
    std::span<char> buffer) {
    auto result = co_await ReceiveAwaiter{*this, buffer};
    if (result.status == OperationStatus::kError) {
        co_return utility::unexpected<std::error_code>(std::move(result.error_code));
    }
    co_return result.bytes;
}
}  // namespace nbio::net




