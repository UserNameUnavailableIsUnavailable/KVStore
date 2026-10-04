#pragma once

#include <cstdint>
#include <span>
#include <system_error>
#include <utility>

#include <NBIO/Utility/Expected.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpSocket.hpp>

namespace NBIO::Net {
class TcpConnector {
   public:
    explicit TcpConnector(SocketAddress::Family family = SocketAddress::Family::kIPv4);

    TcpConnector(const TcpConnector&) = delete;
    TcpConnector& operator=(const TcpConnector&) = delete;

    TcpConnector(TcpConnector&& other) noexcept = default;
    TcpConnector& operator=(TcpConnector&& other) noexcept = default;

    ~TcpConnector() noexcept = default;

    friend void swap(TcpConnector& left, TcpConnector& right) noexcept { left.socket_.swap(right.socket_); }

    Utility::expected<void, std::error_code> bind(const SocketAddress& local) noexcept;
    Utility::expected<void, std::error_code> connect(const SocketAddress& peer) noexcept;
    Utility::expected<void, std::error_code> start_connect(const SocketAddress& peer) noexcept;
    Utility::expected<void, std::error_code> finish_connect() noexcept;
    Utility::expected<std::size_t, std::error_code> send(std::span<const char> buffer) noexcept;
    Utility::expected<std::size_t, std::error_code> receive(std::span<char> buffer) noexcept;
    Utility::expected<void, std::error_code> shutdown(TcpSocket::ShutdownHow how = TcpSocket::ShutdownHow::kBoth) noexcept;

    void close() noexcept;
    bool is_valid() const noexcept { return socket_.is_valid(); }
    std::uintptr_t native_handle() const noexcept { return socket_.native_handle(); }

    Utility::expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;
    Utility::expected<void, std::error_code> reuse_address(bool toggle = true) noexcept;

   private:
    friend class TcpAcceptor;
    explicit TcpConnector(TcpSocket socket) noexcept;

    TcpSocket socket_;
};
}  // namespace NBIO::Net
