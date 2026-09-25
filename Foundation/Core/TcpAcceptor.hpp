#pragma once

#include <cstdint>
#include <system_error>
#include <utility>

#include "TcpConnector.hpp"
#include "TcpSocket.hpp"

namespace Foundation::Core
{
class TcpAcceptor
{
public:
    explicit TcpAcceptor(SocketAddress::Family family = SocketAddress::Family::kIPv4);

    TcpAcceptor(const TcpAcceptor &) = delete;
    TcpAcceptor &operator=(const TcpAcceptor &) = delete;

    expected<void, std::error_code> bind(const SocketAddress &address, std::size_t backlog = 4096) noexcept;

    // Sets `SO_REUSEADDR` on the listener, which has to happen before it is bound: a
    // listener that cannot be restarted while the socket it replaced is still in
    // TIME_WAIT is not much of a listener, and the socket is the acceptor's.
    expected<void, std::error_code> reuse_address(bool toggle = true) noexcept;

    // Waits for a peer to connect and answers the end of it that stays here: a
    // connector that owns the accepted socket and the addresses it was accepted
    // with. What the kernel reports is what comes back, and a caller that made the
    // listener non-blocking gets `EAGAIN` when nobody has asked to be accepted
    // yet -- a plain "nothing there" to come back for, not a failure.
    expected<TcpConnector, std::error_code> accept() noexcept;

    // The connection the kernel made for this listener, from a socket that was
    // accepted on its descriptor. A connector of the accepted kind can only be made
    // by a listener -- the peer is what makes the data path mean anything -- so a
    // channel that accepts on the listener's fd itself, and holds the socket, is the
    // one that has to come back here for the other half. The addresses are read back
    // off the socket.
    TcpConnector adopt(TcpSocket socket) const noexcept;

    expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;

    std::uintptr_t native_handle() const noexcept
    {
        return socket_.native_handle();
    }

    void close() noexcept;
    bool is_valid() const noexcept;

private:
    TcpSocket socket_;
};
} // namespace Foundation::Core