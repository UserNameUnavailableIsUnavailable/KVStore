#pragma once

#include <nbio/async/Task.hpp>
#include <nbio/utility/Expected.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/TcpAcceptor.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/net/TcpAcceptChannel.hpp>
#include <nbio/net/TcpSessionService.hpp>
#include <cstdint>
#include <memory>
#include <system_error>
#include <utility>

namespace nbio::net {
// A listener with the accept channel that waits on it: the server side's way in,
// attached to the engine installed on this thread.
//
// What it answers is the whole of a connection -- the session that will carry it and
// the address it came from -- because accepting is the one way to hold a connection
// whose far end nobody chose, so that address is the one thing about it that cannot be
// looked up on the other side.
//
// The channel is built on the listener and keeps a reference to it, which is why the
// listener is declared first here and why both outlive whatever is served.
class TcpAcceptService final {
   public:
    // Listens on `address`. The listener is bound here, before anything can wait on
    // it, so a service that exists is a service that can accept -- and a bind the
    // kernel refuses is a failure to start rather than one to discover later.
    TcpAcceptService(const nbio::net::Address& address, int backlog = 4096);

    TcpAcceptService(const TcpAcceptService&) = delete;
    TcpAcceptService& operator=(const TcpAcceptService&) = delete;
    TcpAcceptService(TcpAcceptService&&) = delete;
    TcpAcceptService& operator=(TcpAcceptService&&) = delete;

    ~TcpAcceptService() noexcept;

    nbio::async::Task<nbio::runtime, 
        utility::expected<std::pair<std::shared_ptr<TcpSessionService>, nbio::net::Address>, std::error_code>>
    Accept();

    nbio::net::TcpAcceptor& acceptor() noexcept { return acceptor_; }

    const nbio::net::TcpAcceptor& acceptor() const noexcept { return acceptor_; }

   private:
    nbio::net::TcpAcceptor acceptor_;
    TcpAcceptChannel channel_;
};
}  // namespace nbio::net




