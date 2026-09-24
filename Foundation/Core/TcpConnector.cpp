#include "TcpConnector.hpp"

namespace Foundation::Core
{
TcpConnector::TcpConnector(SocketAddress::Family family) :
    socket_(family, TcpSocket::Type::kStream)
{
}

TcpConnector::TcpConnector(TcpSocket socket) noexcept :
    socket_(std::move(socket))
{
    // The socket arrives connected or accepted, so both lookups normally succeed.
    capture_addresses();
}

expected<void, std::error_code> TcpConnector::bind(const SocketAddress &local) noexcept
{
    return socket_.bind(local);
}

expected<void, std::error_code> TcpConnector::connect(const SocketAddress &peer) noexcept
{
    if (auto result = socket_.connect(peer); !result) [[unlikely]]
    {
        return unexpected<std::error_code>(result.error());
    }
    // The connection is up, so this is where the two addresses become known: before
    // it there was a socket and a peer to reach, and after it there is a connection
    // with an end on each side.
    capture_addresses();
    return {};
}

expected<std::size_t, std::error_code> TcpConnector::send(std::span<const char> buffer) noexcept
{
    return socket_.send(buffer);
}

expected<std::size_t, std::error_code> TcpConnector::receive(std::span<char> buffer) noexcept
{
    return socket_.receive(buffer);
}

expected<void, std::error_code> TcpConnector::shutdown(TcpSocket::ShutdownHow how) noexcept
{
    // `TcpSocket::shutdown` disables the requested direction(s) and releases the
    // descriptor, reporting nothing, so there is no error to forward; the
    // signature keeps the door open for a platform that does report one.
    socket_.shutdown(how);
    clear_addresses();
    return {};
}

void TcpConnector::close() noexcept
{
    socket_.close();
    // The addresses went with the descriptor. A closed connection that still
    // reported one would be saying something that is no longer true, and the
    // accessors have no way of saying it is not.
    clear_addresses();
}

expected<void, std::error_code> TcpConnector::non_blocking(bool toggle) noexcept
{
    return socket_.non_blocking(toggle);
}

expected<void, std::error_code> TcpConnector::reuse_address(bool toggle) noexcept
{
    return socket_.reuse_address(toggle);
}

void TcpConnector::capture_addresses() noexcept
{
    // A failure would mean the socket is not what this object was promised, and
    // there is nowhere to report it from here: `connect` and the constructor the
    // acceptor uses are `noexcept`, and the accessors return a reference. The
    // address it is about is left invalid instead, which is what
    // `SocketAddress::is_valid` is for.
    if (auto address = socket_.local_address())
    {
        local_address_ = *address;
    }
    if (auto address = socket_.peer_address())
    {
        peer_address_ = *address;
    }
}

void TcpConnector::clear_addresses() noexcept
{
    local_address_ = SocketAddress{};
    peer_address_ = SocketAddress{};
}
} // namespace Foundation::Core
