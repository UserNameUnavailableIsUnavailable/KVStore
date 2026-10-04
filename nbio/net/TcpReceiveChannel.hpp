#pragma once

#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/utility/Buffer.hpp>
#include <nbio/net/TcpConnector.hpp>
#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/net/Payload.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <deque>
#include <span>
#include <system_error>

namespace nbio::net {
class ReceiveAwaiter;

class TcpReceiveChannel final : public nbio::core::Channel<TcpReceiveChannel> {
   public:
    using Payload = detail::MessagePayload<TcpReceiveChannel>;

    explicit TcpReceiveChannel(nbio::net::TcpConnector& connector, nbio::core::Multiplexer& multiplexer,
                               nbio::async::Scheduler& scheduler);
    ~TcpReceiveChannel() noexcept;

    nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> Receive(std::span<char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload& Submit();
    void Complete();

    net::TcpConnector& connector() noexcept { return connector_; }
    const net::TcpConnector& connector() const noexcept { return connector_; }

   private:
    friend class ReceiveAwaiter;

    // Queues the receive and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void Prepare(async::Coroutine waiter, net::Transmission* transmission);

    nbio::net::TcpConnector& connector_;
    // One waiter per transmission, in queue order.
    std::deque<async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace nbio::net




