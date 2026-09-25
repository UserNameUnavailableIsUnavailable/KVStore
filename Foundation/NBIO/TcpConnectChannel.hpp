#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/TcpConnector.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Types.hpp>

#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
class ConnectAwaiter;

// One attempt at a connection, over a connector that is handed in holding its socket
// and handed back once the connection is made. The channel is spent afterwards: a
// connect that has happened is not one to wait on again, so a new attempt means a new
// channel -- which is why the service that uses this makes one per connect rather
// than keeping one.
//
// The connector goes in as well as out because a channel has to know the descriptor
// it watches when it is constructed, that being the only thing the multiplexer is
// told about it: a socket made inside the channel would exist too late to say so.
//
// The wait itself is a poll for writability. A non-blocking connect leaves the
// handshake to the kernel, and the socket becoming writable is the kernel saying the
// handshake is over; what "over" means -- made, refused, reset -- is a question the
// socket answers itself, which is why the verdict is read once the wait is answered
// rather than taken from the readiness.
class TcpConnectChannel final : public Foundation::NBIO::Channel
{
  public:
    TcpConnectChannel(Foundation::Core::TcpConnector connector, Foundation::NBIO::Multiplexer &multiplexer,
                      Foundation::Async::Scheduler &scheduler);
    ~TcpConnectChannel() noexcept;

    // Establishes the connection, or answers why there is none.
    Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> connect(
        const Foundation::Core::SocketAddress &target);

    // The same, with the local address pinned first: a caller that cares which end it
    // comes from says so, and one that does not leaves it to the kernel.
    Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> connect(
        const Foundation::Core::SocketAddress &source, const Foundation::Core::SocketAddress &target);

    // The operation the backend performs is a one-shot poll; the payload carries only
    // whether one is already out there.
    Payload &submit();
    void complete();

  private:
    friend class ConnectAwaiter;

    void park(Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    // The connection, once it has been made: the channel has nothing left to do with
    // it, so what it gives back is the socket it was given.
    Foundation::Core::TcpConnector take_connector() noexcept
    {
        return std::move(connector_);
    }

    Foundation::Core::TcpConnector connector_;
    Async::Coroutine waiter_{};
    Payload payload_{ConnectPayload{}};
};
} // namespace Foundation::NBIO
