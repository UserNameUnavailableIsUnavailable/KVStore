#include "TcpAcceptor.hpp"

namespace Foundation::Core
{
TcpAcceptor::TcpAcceptor(SocketAddress::Family family) :
    socket_(family, TcpSocket::Type::kStream)
{
}

expected<void, std::error_code> TcpAcceptor::bind(const SocketAddress &address, std::size_t backlog) noexcept
{
    if (auto result = socket_.bind(address); !result) [[unlikely]]
    {
        return unexpected<std::error_code>(result.error());
    }
    // `listen` takes the backlog as an `int`; the cast is safe for every backlog
    // the kernel accepts, and a larger one would be clamped by it anyway.
    return socket_.listen(static_cast<int>(backlog));
}

expected<TcpConnector, std::error_code> TcpAcceptor::accept() noexcept
{
    auto result = socket_.accept();
    if (!result) [[unlikely]]
    {
        return unexpected<std::error_code>(result.error());
    }
    // The accepted socket is already connected to the peer it was accepted from, so
    // it moves straight into a connector -- which is where the two addresses are
    // read back off it. The peer address the kernel handed over with the socket is
    // the same one `getpeername` answers with, so nothing is lost by taking it from
    // there instead.
    return TcpConnector(std::move(result->first));
}

expected<void, std::error_code> TcpAcceptor::non_blocking(bool toggle) noexcept
{
    return socket_.non_blocking(toggle);
}

void TcpAcceptor::close() noexcept
{
    socket_.close();
}

bool TcpAcceptor::is_valid() const noexcept
{
    return socket_.is_valid();
}
} // namespace Foundation::Core