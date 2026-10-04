#pragma once

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpAcceptor.hpp>
#include <NBIO/Net/TcpConnector.hpp>
#include <NBIO/Net/TcpSocket.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Net/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Core/Types.hpp>
#include <deque>
#include <optional>
#include <utility>

namespace NBIO::Net {
class AcceptAwaiter;

class TcpAcceptChannel final : public NBIO::Core::Channel<TcpAcceptChannel> {
   public:
    using Payload = detail::CommunicationPayload<TcpAcceptChannel>;

    TcpAcceptChannel(NBIO::Net::TcpAcceptor& acceptor, NBIO::Core::Multiplexer& multiplexer,
                     NBIO::Async::Scheduler& scheduler);
    ~TcpAcceptChannel() noexcept;

    // The next peer that connects, as the connection it arrived on and the address it
    // came from: the socket is accepted on the listener's descriptor by the backend,
    // and the listener is what turns it into a connector -- a connection of the
    // accepted kind can only be made by a listener, because the peer is what makes
    // its data path mean anything.
    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::pair<Net::TcpConnector, Net::SocketAddress>, std::error_code>>
    accept();

    // The operation the backend is asked to perform lives in the payload; the
    // backend fills the communication slots and asks the channel to reap them.
    Payload& submit();
    void complete();

    Net::TcpAcceptor& acceptor() noexcept { return acceptor_; }
    const Net::TcpAcceptor& acceptor() const noexcept { return acceptor_; }

   private:
    friend class AcceptAwaiter;

    // Queues the wait and arms the channel: this is the suspension point, and being
    // armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, Net::Communication* result);

    NBIO::Net::TcpAcceptor& acceptor_;
    // One waiter per communication slot, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace NBIO::Net




