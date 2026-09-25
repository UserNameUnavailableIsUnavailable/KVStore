#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/TcpAcceptor.hpp>
#include <Foundation/Core/TcpConnector.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/TcpSocket.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <deque>
#include <optional>
#include <utility>

namespace Foundation::NBIO
{
class AcceptAwaiter;

class TcpAcceptChannel final : public Foundation::NBIO::Channel
{
  public:
    TcpAcceptChannel(Foundation::Core::TcpAcceptor &acceptor, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~TcpAcceptChannel() noexcept;

    // The next peer that connects, as the connection it arrived on and the address it
    // came from: the socket is accepted on the listener's descriptor by the backend,
    // and the listener is what turns it into a connector -- a connection of the
    // accepted kind can only be made by a listener, because the peer is what makes
    // its data path mean anything.
    Foundation::NBIO::Task<Core::expected<std::pair<Core::TcpConnector, Core::SocketAddress>, std::error_code>> accept();

    // The operation the backend is asked to perform lives in the payload; the
    // backend fills the communication slots and asks the channel to reap them.
    Payload &submit();
    void complete();

    Foundation::Core::TcpAcceptor &acceptor() noexcept
    {
        return acceptor_;
    }
    const Foundation::Core::TcpAcceptor &acceptor() const noexcept
    {
        return acceptor_;
    }

  private:
    friend class AcceptAwaiter;

    // Queues the wait and arms the channel: this is the suspension point, and being
    // armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, Core::Communication *result);

    Foundation::Core::TcpAcceptor &acceptor_;
    // One waiter per communication slot, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{AcceptPayload{}};
};
} // namespace Foundation::NBIO
