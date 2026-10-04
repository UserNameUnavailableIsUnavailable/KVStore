#include "TcpConnector.hpp"

namespace nbio::net {
TcpConnector::TcpConnector(Address::Family family) : socket_(family, TcpSocket::Type::kStream) {}

TcpConnector::TcpConnector(TcpSocket socket) noexcept : socket_(std::move(socket)) {}

utility::expected<void, std::error_code> TcpConnector::Bind(const Address& local) noexcept { return socket_.Bind(local); }

utility::expected<void, std::error_code> TcpConnector::Connect(const Address& peer) noexcept {
    if (auto started = StartConnect(peer); !started) [[unlikely]] {
        return started;
    }
    return FinishConnect();
}

utility::expected<void, std::error_code> TcpConnector::StartConnect(const Address& peer) noexcept {
    if (auto connected = socket_.Connect(peer); !connected) [[unlikely]] {
        const std::error_code& why = connected.error();
        if (why != std::errc::operation_in_progress && why != std::errc::connection_already_in_progress) {
            return utility::unexpected<std::error_code>(why);
        }
    }
    return {};
}

utility::expected<void, std::error_code> TcpConnector::FinishConnect() noexcept {
    if (auto settled = socket_.TakeError(); !settled) [[unlikely]] {
        return settled;
    }
    return {};
}

utility::expected<std::size_t, std::error_code> TcpConnector::Send(std::span<const char> buffer) noexcept {
    return socket_.Send(buffer);
}

utility::expected<std::size_t, std::error_code> TcpConnector::Receive(std::span<char> buffer) noexcept {
    return socket_.Receive(buffer);
}

utility::expected<void, std::error_code> TcpConnector::Shutdown(TcpSocket::ShutdownHow how) noexcept {
    socket_.Shutdown(how);
    return {};
}

void TcpConnector::Close() noexcept { socket_.Close(); }

utility::expected<void, std::error_code> TcpConnector::NonBlocking(bool toggle) noexcept { return socket_.NonBlocking(toggle); }

utility::expected<void, std::error_code> TcpConnector::ReuseAddress(bool toggle) noexcept { return socket_.ReuseAddress(toggle); }
}  // namespace nbio::net
