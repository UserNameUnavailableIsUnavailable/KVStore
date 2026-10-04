#pragma once

#include <cstdint>
#include <system_error>
#include <utility>

#include <nbio/utility/Expected.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/TcpConnector.hpp>
#include <nbio/net/TcpSocket.hpp>

namespace nbio::net {
class TcpAcceptor {
   public:
    explicit TcpAcceptor(Address::Family family = Address::Family::kIPv4);

    TcpAcceptor(const TcpAcceptor&) = delete;
    TcpAcceptor& operator=(const TcpAcceptor&) = delete;

    utility::expected<void, std::error_code> Bind(const Address& address, std::size_t backlog = 4096) noexcept;
    utility::expected<void, std::error_code> ReuseAddress(bool toggle = true) noexcept;
    utility::expected<TcpConnector, std::error_code> Accept() noexcept;
    TcpConnector adopt(TcpSocket socket) const noexcept;
    utility::expected<void, std::error_code> NonBlocking(bool toggle = true) noexcept;

    std::uintptr_t native_handle() const noexcept { return socket_.native_handle(); }

    void Close() noexcept;
    bool IsValid() const noexcept;

   private:
    TcpSocket socket_;
};
}  // namespace nbio::net
