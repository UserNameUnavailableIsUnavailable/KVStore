#include "TcpAcceptor.hpp"

namespace NBIO::Net {
TcpAcceptor::TcpAcceptor(SocketAddress::Family family) : socket_(family, TcpSocket::Type::kStream) {}

Utility::expected<void, std::error_code> TcpAcceptor::bind(const SocketAddress& address, std::size_t backlog) noexcept {
    if (auto result = socket_.bind(address); !result) [[unlikely]] {
        return Utility::unexpected<std::error_code>(result.error());
    }
    return socket_.listen(static_cast<int>(backlog));
}

Utility::expected<TcpConnector, std::error_code> TcpAcceptor::accept() noexcept {
    auto result = socket_.accept();
    if (!result) [[unlikely]] {
        return Utility::unexpected<std::error_code>(result.error());
    }
    return TcpConnector(std::move(result->first));
}

TcpConnector TcpAcceptor::adopt(TcpSocket socket) const noexcept { return TcpConnector(std::move(socket)); }

Utility::expected<void, std::error_code> TcpAcceptor::reuse_address(bool toggle) noexcept { return socket_.reuse_address(toggle); }

Utility::expected<void, std::error_code> TcpAcceptor::non_blocking(bool toggle) noexcept { return socket_.non_blocking(toggle); }

void TcpAcceptor::close() noexcept { socket_.close(); }

bool TcpAcceptor::is_valid() const noexcept { return socket_.is_valid(); }
}  // namespace NBIO::Net
