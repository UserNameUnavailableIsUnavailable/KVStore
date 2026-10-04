#include "TcpConnector.hpp"

namespace NBIO::Net {
TcpConnector::TcpConnector(SocketAddress::Family family) : socket_(family, TcpSocket::Type::kStream) {}

TcpConnector::TcpConnector(TcpSocket socket) noexcept : socket_(std::move(socket)) {}

Utility::expected<void, std::error_code> TcpConnector::bind(const SocketAddress& local) noexcept { return socket_.bind(local); }

Utility::expected<void, std::error_code> TcpConnector::connect(const SocketAddress& peer) noexcept {
    if (auto started = start_connect(peer); !started) [[unlikely]] {
        return started;
    }
    return finish_connect();
}

Utility::expected<void, std::error_code> TcpConnector::start_connect(const SocketAddress& peer) noexcept {
    if (auto connected = socket_.connect(peer); !connected) [[unlikely]] {
        const std::error_code& why = connected.error();
        if (why != std::errc::operation_in_progress && why != std::errc::connection_already_in_progress) {
            return Utility::unexpected<std::error_code>(why);
        }
    }
    return {};
}

Utility::expected<void, std::error_code> TcpConnector::finish_connect() noexcept {
    if (auto settled = socket_.take_error(); !settled) [[unlikely]] {
        return settled;
    }
    return {};
}

Utility::expected<std::size_t, std::error_code> TcpConnector::send(std::span<const char> buffer) noexcept {
    return socket_.send(buffer);
}

Utility::expected<std::size_t, std::error_code> TcpConnector::receive(std::span<char> buffer) noexcept {
    return socket_.receive(buffer);
}

Utility::expected<void, std::error_code> TcpConnector::shutdown(TcpSocket::ShutdownHow how) noexcept {
    socket_.shutdown(how);
    return {};
}

void TcpConnector::close() noexcept { socket_.close(); }

Utility::expected<void, std::error_code> TcpConnector::non_blocking(bool toggle) noexcept { return socket_.non_blocking(toggle); }

Utility::expected<void, std::error_code> TcpConnector::reuse_address(bool toggle) noexcept { return socket_.reuse_address(toggle); }
}  // namespace NBIO::Net
