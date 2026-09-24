#pragma once

#include <cstdint>
#include <system_error>
#include <utility>

#include "TcpConnector.hpp"
#include "TcpSocket.hpp"

namespace Foundation::Core
{
class TcpAcceptor
{
public:
    explicit TcpAcceptor(SocketAddress::Family family = SocketAddress::Family::kIPv4);

    TcpAcceptor(const TcpAcceptor &) = delete;
    TcpAcceptor &operator=(const TcpAcceptor &) = delete;

    expected<void, std::error_code> bind(const SocketAddress &address, std::size_t backlog = 4096) noexcept;

    // Waits for a peer to connect and answers the end of it that stays here: a
    // connector that owns the accepted socket and the addresses it was accepted
    // with. What the kernel reports is what comes back, and a caller that made the
    // listener non-blocking gets `EAGAIN` when nobody has asked to be accepted
    // yet -- a plain "nothing there" to come back for, not a failure.
    expected<TcpConnector, std::error_code> accept() noexcept;

    expected<void, std::error_code> non_blocking(bool toggle = true) noexcept;

    std::uintptr_t native_handle() const noexcept
    {
        return socket_.native_handle();
    }

    void close() noexcept;
    bool is_valid() const noexcept;

private:
    TcpSocket socket_;
};
} // namespace Foundation::Core