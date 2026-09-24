#pragma once

#include <cstdint>
#include <span>
#include <system_error>
#include <utility>

#include "Expected.hpp"
#include "SocketAddress.hpp"
#include "TcpSocket.hpp"

namespace Foundation::Core
{
class TcpConnector
{
  public:
    // The client's end: a stream socket of its own, not connected to anything yet.
    // Only construction may throw, and it does when the kernel refuses a socket.
    explicit TcpConnector(SocketAddress::Family family = SocketAddress::Family::kIPv4);

    TcpConnector(const TcpConnector &) = delete;
    TcpConnector &operator=(const TcpConnector &) = delete;

    TcpConnector(TcpConnector &&other) noexcept = default;
    TcpConnector &operator=(TcpConnector &&other) noexcept = default;

    ~TcpConnector() noexcept = default;

    friend void swap(TcpConnector &left, TcpConnector &right) noexcept
    {
        left.socket_.swap(right.socket_);
        std::swap(left.local_address_, right.local_address_);
        std::swap(left.peer_address_, right.peer_address_);
    }

    // Pins the local address this connection comes from, before connecting. Left
    // out, the kernel picks along the route.
    expected<void, std::error_code> bind(const SocketAddress &local) noexcept;

    // Connects to a peer and captures the addresses the connection ended up with.
    // What is left is a connection that can send, receive and close.
    expected<void, std::error_code> connect(const SocketAddress &peer) noexcept;

    // Returns the number of bytes handed to the kernel, which may be less than
    // `buffer.size()`. A peer that went away is a real failure here, reported as
    // `std::errc::broken_pipe` or `std::errc::connection_reset` by the socket.
    expected<std::size_t, std::error_code> send(std::span<const char> buffer) noexcept;

    // Returns the number of bytes read, or 0 once the peer has closed the stream.
    expected<std::size_t, std::error_code> receive(std::span<char> buffer) noexcept;

    // Disables one or both directions and releases the descriptor with them, so the
    // connection is over afterwards. Neither `::shutdown` nor the socket's wrapper
    // reports a result, so this is always a value; it exists so the operation keeps
    // the shape every other one here has.
    expected<void, std::error_code> shutdown(TcpSocket::ShutdownHow how = TcpSocket::ShutdownHow::kBoth) noexcept;

    void close() noexcept;

    bool is_valid() const noexcept
    {
        return socket_.is_valid();
    }

    std::uintptr_t native_handle() const noexcept
    {
        return socket_.native_handle();
    }

    expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;
    expected<void, std::error_code> reuse_address(bool toggle = true) noexcept;

    // The addresses captured when the connection was established, invalid before
    // that and after it is over. They are cached because these accessors are
    // `noexcept` and cannot report a failing `getsockname`/`getpeername`: a lookup
    // that fails leaves the address it is about default-constructed, which
    // `SocketAddress::is_valid` reports.
    const SocketAddress &local_address() const noexcept
    {
        return local_address_;
    }

    const SocketAddress &peer_address() const noexcept
    {
        return peer_address_;
    }

  private:
    friend class TcpAcceptor;

    // The acceptor's end: a socket the kernel has already connected to a peer. That
    // peer is what makes the data path here mean anything, which is why this is
    // private and the listener is the one that can call it.
    explicit TcpConnector(TcpSocket socket) noexcept;

    void capture_addresses() noexcept;
    void clear_addresses() noexcept;

    TcpSocket socket_;
    SocketAddress local_address_;
    SocketAddress peer_address_;
};
} // namespace Foundation::Core
