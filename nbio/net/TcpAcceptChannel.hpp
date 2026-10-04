#pragma once

#include <nbio/async/Coroutine.hpp>
#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/TcpAcceptor.hpp>
#include <nbio/net/TcpConnector.hpp>
#include <nbio/net/TcpSocket.hpp>
#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/net/Payload.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/core/Types.hpp>
#include <deque>
#include <optional>
#include <utility>

namespace nbio::net {
class AcceptAwaiter;

class TcpAcceptChannel final : public nbio::core::Channel<TcpAcceptChannel> {
   public:
    using Payload = detail::CommunicationPayload<TcpAcceptChannel>;

    TcpAcceptChannel(nbio::net::TcpAcceptor& acceptor, nbio::core::Multiplexer& multiplexer,
                     nbio::async::Scheduler& scheduler);
    ~TcpAcceptChannel() noexcept;

    // The next peer that connects, as the connection it arrived on and the address it
    // came from: the socket is accepted on the listener's descriptor by the backend,
    // and the listener is what turns it into a connector -- a connection of the
    // accepted kind can only be made by a listener, because the peer is what makes
    // its data path mean anything.
    nbio::async::Task<nbio::runtime, utility::expected<std::pair<net::TcpConnector, net::Address>, std::error_code>>
    Accept();

    // The operation the backend is asked to perform lives in the payload; the
    // backend fills the communication slots and asks the channel to reap them.
    Payload& Submit();
    void Complete();

    net::TcpAcceptor& acceptor() noexcept { return acceptor_; }
    const net::TcpAcceptor& acceptor() const noexcept { return acceptor_; }

   private:
    friend class AcceptAwaiter;

    // Queues the wait and arms the channel: this is the suspension point, and being
    // armed is what tells the backend to look at the channel.
    void Prepare(async::Coroutine waiter, net::Communication* result);

    nbio::net::TcpAcceptor& acceptor_;
    // One waiter per communication slot, in queue order.
    std::deque<async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace nbio::net




