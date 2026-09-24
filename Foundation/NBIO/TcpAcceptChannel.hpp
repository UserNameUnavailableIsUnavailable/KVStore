#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/SocketAddress.hpp>
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
    TcpAcceptChannel(Foundation::Core::TcpSocket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~TcpAcceptChannel() noexcept;

    Foundation::NBIO::Task<Core::expected<std::pair<Core::TcpSocket, Core::SocketAddress>, std::error_code>> accept();

    // The operation the backend is asked to perform lives in the payload; the
    // backend fills the communication slots and asks the channel to reap them.
    Payload &submit();
    void complete();

    Foundation::Core::TcpSocket &socket() noexcept
    {
        return listener_;
    }
    const Foundation::Core::TcpSocket &socket() const noexcept
    {
        return listener_;
    }

  private:
    friend class AcceptAwaiter;

    // Queues the wait and arms the channel: this is the suspension point, and being
    // armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, Core::Communication *result);

    Foundation::Core::TcpSocket listener_;
    // One waiter per communication slot, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{AcceptPayload{}};
};
} // namespace Foundation::NBIO
