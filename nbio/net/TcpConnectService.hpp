#pragma once

#include <nbio/async/Task.hpp>
#include <nbio/utility/Expected.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/TcpSessionService.hpp>
#include <memory>
#include <system_error>

namespace nbio::net {
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
        nbio::net::Address::Family family = nbio::net::Address::Family::kIPv4);

    TcpConnectService(const TcpConnectService&) = delete;
    TcpConnectService& operator=(const TcpConnectService&) = delete;
    TcpConnectService(TcpConnectService&&) = delete;
    TcpConnectService& operator=(TcpConnectService&&) = delete;

    ~TcpConnectService() noexcept = default;

    // A connection to `target`, and the session that will carry it.
    nbio::async::Task<nbio::runtime, utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> Connect(
        const nbio::net::Address& target);

    // The same, with the local address pinned first: a caller that cares which end it
    // comes from says so, and one that does not leaves it to the kernel.
    nbio::async::Task<nbio::runtime, utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> Connect(
        const nbio::net::Address& source, const nbio::net::Address& target);

   private:
    nbio::net::Address::Family family_;
};
}  // namespace nbio::net




