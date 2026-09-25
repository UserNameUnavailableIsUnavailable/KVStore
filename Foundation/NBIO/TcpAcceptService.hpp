#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/TcpAcceptor.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/TcpAcceptChannel.hpp>
#include <Foundation/NBIO/TcpSessionService.hpp>

#include <cstdint>
#include <memory>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
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
class TcpAcceptService final
{
  public:
    // Listens on `address`. The listener is bound here, before anything can wait on
    // it, so a service that exists is a service that can accept -- and a bind the
    // kernel refuses is a failure to start rather than one to discover later.
    TcpAcceptService(const Foundation::Core::SocketAddress &address, int backlog = 4096);

    TcpAcceptService(const TcpAcceptService &) = delete;
    TcpAcceptService &operator=(const TcpAcceptService &) = delete;
    TcpAcceptService(TcpAcceptService &&) = delete;
    TcpAcceptService &operator=(TcpAcceptService &&) = delete;

    ~TcpAcceptService() noexcept;

    Foundation::NBIO::Task<
        Core::expected<std::pair<std::shared_ptr<TcpSessionService>, Foundation::Core::SocketAddress>, std::error_code>>
    accept();

    Foundation::Core::TcpAcceptor &acceptor() noexcept
    {
        return acceptor_;
    }

    const Foundation::Core::TcpAcceptor &acceptor() const noexcept
    {
        return acceptor_;
    }

  private:
    Foundation::Core::TcpAcceptor acceptor_;
    TcpAcceptChannel channel_;
};
} // namespace Foundation::NBIO
