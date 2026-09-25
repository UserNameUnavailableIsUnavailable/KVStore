#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/NBIO/TcpSessionService.hpp>

#include <memory>
#include <system_error>

namespace Foundation::NBIO
{
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
class TcpConnectService final
{
  public:
    explicit TcpConnectService(Foundation::Core::SocketAddress::Family family = Foundation::Core::SocketAddress::Family::kIPv4);

    TcpConnectService(const TcpConnectService &) = delete;
    TcpConnectService &operator=(const TcpConnectService &) = delete;
    TcpConnectService(TcpConnectService &&) = delete;
    TcpConnectService &operator=(TcpConnectService &&) = delete;

    ~TcpConnectService() noexcept = default;

    // A connection to `target`, and the session that will carry it.
    Foundation::NBIO::Task<Core::expected<std::shared_ptr<TcpSessionService>, std::error_code>> connect(
        const Foundation::Core::SocketAddress &target);

    // The same, with the local address pinned first: a caller that cares which end it
    // comes from says so, and one that does not leaves it to the kernel.
    Foundation::NBIO::Task<Core::expected<std::shared_ptr<TcpSessionService>, std::error_code>> connect(
        const Foundation::Core::SocketAddress &source, const Foundation::Core::SocketAddress &target);

  private:
    Foundation::Core::SocketAddress::Family family_;
};
} // namespace Foundation::NBIO
