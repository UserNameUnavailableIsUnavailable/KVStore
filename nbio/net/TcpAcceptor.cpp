#include "TcpAcceptor.hpp"

namespace nbio::net {
TcpAcceptor::TcpAcceptor(Address::Family family) : socket_(family, TcpSocket::Type::kStream) {}

utility::expected<void, std::error_code> TcpAcceptor::Bind(const Address& address, std::size_t backlog) noexcept {
    if (auto result = socket_.Bind(address); !result) [[unlikely]] {
        return utility::unexpected<std::error_code>(result.error());
    }
    return socket_.Listen(static_cast<int>(backlog));
}

utility::expected<TcpConnector, std::error_code> TcpAcceptor::Accept() noexcept {
    auto result = socket_.Accept();
    if (!result) [[unlikely]] {
        return utility::unexpected<std::error_code>(result.error());
    }
    return TcpConnector(std::move(result->first));
}

TcpConnector TcpAcceptor::adopt(TcpSocket socket) const noexcept { return TcpConnector(std::move(socket)); }

utility::expected<void, std::error_code> TcpAcceptor::ReuseAddress(bool toggle) noexcept { return socket_.ReuseAddress(toggle); }

utility::expected<void, std::error_code> TcpAcceptor::NonBlocking(bool toggle) noexcept { return socket_.NonBlocking(toggle); }

void TcpAcceptor::Close() noexcept { socket_.Close(); }

bool TcpAcceptor::IsValid() const noexcept { return socket_.IsValid(); }
}  // namespace nbio::net
