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
    // Nothing to do: the socket arrives connected or accepted, and where the two
    // ends are is the caller's business -- whoever made the connection is the one
    // that knows the addresses it asked for.
}

expected<void, std::error_code> TcpConnector::bind(const SocketAddress &local) noexcept
{
    return socket_.bind(local);
}

expected<void, std::error_code> TcpConnector::connect(const SocketAddress &peer) noexcept
{
    if (auto started = start_connect(peer); !started) [[unlikely]]
    {
        return started;
    }
    // The handshake is the kernel's from here, and on a blocking socket it has
    // happened by the time the call above returned: what is left for either kind is
    // the verdict, which finish_connect() reads.
    return finish_connect();
}

expected<void, std::error_code> TcpConnector::start_connect(const SocketAddress &peer) noexcept
{
    if (auto connected = socket_.connect(peer); !connected) [[unlikely]]
    {
        // A connect that is under way is what a non-blocking socket reports instead
        // of blocking. That is not a failure and not a finished handshake either: it
        // is the socket saying the kernel has it now.
        const std::error_code &why = connected.error();
        if (why != std::errc::operation_in_progress && why != std::errc::connection_already_in_progress)
        {
            return unexpected<std::error_code>(why);
        }
    }
    return {};
}

expected<void, std::error_code> TcpConnector::finish_connect() noexcept
{
    if (auto settled = socket_.take_error(); !settled) [[unlikely]]
    {
        return settled;
    }
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
    return {};
}

void TcpConnector::close() noexcept
{
    socket_.close();
}

expected<void, std::error_code> TcpConnector::non_blocking(bool toggle) noexcept
{
    return socket_.non_blocking(toggle);
}

expected<void, std::error_code> TcpConnector::reuse_address(bool toggle) noexcept
{
    return socket_.reuse_address(toggle);
}
} // namespace Foundation::Core
