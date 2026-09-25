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
    }

    // Pins the local address this connection comes from, before connecting. Left
    // out, the kernel picks along the route.
    expected<void, std::error_code> bind(const SocketAddress &local) noexcept;

    // Connects to a peer and captures the addresses the connection ended up with.
    // What is left is a connection that can send, receive and close.
    //
    // This is the form for a socket that blocks: the handshake has finished -- or
    // failed -- by the time it returns. It cannot be used on a socket that has been
    // made non-blocking, because there would be nothing here to wait in; that is
    // what the two halves below are for.
    expected<void, std::error_code> connect(const SocketAddress &peer) noexcept;

    // Begins a connection: the peer is looked up and the SYN goes out. Whether the
    // handshake has finished when this returns is the socket's business -- a blocking
    // socket has finished it, a non-blocking one reports a connect under way and has
    // not -- and either way the rest is the kernel's. finish_connect() reports how it
    // went, so a caller with an event loop can wait for the socket to become writable
    // instead of waiting in here.
    expected<void, std::error_code> start_connect(const SocketAddress &peer) noexcept;

    // Reports how a connection begun with start_connect() ended, and captures the two
    // addresses it ended with. Only meaningful once the socket is writable: before
    // that the handshake is still in flight, and a socket that has been neither
    // refused nor made has no error to report either.
    expected<void, std::error_code> finish_connect() noexcept;

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

  private:
    friend class TcpAcceptor;

    // The acceptor's end: a socket the kernel has already connected to a peer. That
    // peer is what makes the data path here mean anything, which is why this is
    // private and the listener is the one that can call it.
    explicit TcpConnector(TcpSocket socket) noexcept;

    TcpSocket socket_;
};
} // namespace Foundation::Core
