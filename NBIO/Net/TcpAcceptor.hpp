#pragma once

#include <cstdint>
#include <system_error>
#include <utility>

#include <NBIO/Utility/Expected.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpConnector.hpp>
#include <NBIO/Net/TcpSocket.hpp>

namespace NBIO::Net {
class TcpAcceptor {
   public:
    explicit TcpAcceptor(SocketAddress::Family family = SocketAddress::Family::kIPv4);

    TcpAcceptor(const TcpAcceptor&) = delete;
    TcpAcceptor& operator=(const TcpAcceptor&) = delete;

    Utility::expected<void, std::error_code> bind(const SocketAddress& address, std::size_t backlog = 4096) noexcept;
    Utility::expected<void, std::error_code> reuse_address(bool toggle = true) noexcept;
    Utility::expected<TcpConnector, std::error_code> accept() noexcept;
    TcpConnector adopt(TcpSocket socket) const noexcept;
    Utility::expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;

    std::uintptr_t native_handle() const noexcept { return socket_.native_handle(); }

    void close() noexcept;
    bool is_valid() const noexcept;

   private:
    TcpSocket socket_;
};
}  // namespace NBIO::Net
