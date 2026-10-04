#pragma once

#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Expected.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpSessionService.hpp>
#include <memory>
#include <system_error>

namespace NBIO::Net {
// The client side's way in: it holds no connection and no channel, only what a
// connection is made of, and each call makes one.
//
// That is the whole of it. A connect channel is spent once it has produced a
// connector -- a connect that has happened is not one to wait on again -- so the
// channel is made inside the call and gone when it returns, and what the caller is
// left holding is the session.
//
// Attached to the engine installed on this thread, which is where the connection it
// makes will be read and written from.
class TcpConnectService final {
   public:
    explicit TcpConnectService(
        NBIO::Net::SocketAddress::Family family = NBIO::Net::SocketAddress::Family::kIPv4);

    TcpConnectService(const TcpConnectService&) = delete;
    TcpConnectService& operator=(const TcpConnectService&) = delete;
    TcpConnectService(TcpConnectService&&) = delete;
    TcpConnectService& operator=(TcpConnectService&&) = delete;

    ~TcpConnectService() noexcept = default;

    // A connection to `target`, and the session that will carry it.
    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> connect(
        const NBIO::Net::SocketAddress& target);

    // The same, with the local address pinned first: a caller that cares which end it
    // comes from says so, and one that does not leaves it to the kernel.
    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> connect(
        const NBIO::Net::SocketAddress& source, const NBIO::Net::SocketAddress& target);

   private:
    NBIO::Net::SocketAddress::Family family_;
};
}  // namespace NBIO::Net




