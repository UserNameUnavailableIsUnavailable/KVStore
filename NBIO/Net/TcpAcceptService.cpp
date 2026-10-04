#include "TcpAcceptService.hpp"

#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace NBIO::Net {
TcpAcceptService::TcpAcceptService(const NBIO::Net::SocketAddress& address, int backlog)
    : acceptor_(address.family()), channel_(acceptor_, NBIO::Runtime::multiplexer(), NBIO::Runtime::scheduler()) {
    // A listener that cannot be restarted while the socket it replaced is still in
    // TIME_WAIT is not much of a listener, and nothing else can set this: the socket
    // belongs to the acceptor.
    if (auto reuse = acceptor_.reuse_address(true); !reuse) [[unlikely]]
    {
        throw std::system_error(reuse.error(), "setsockopt(SO_REUSEADDR) failed");
    }
    if (auto bound = acceptor_.bind(address, static_cast<std::size_t>(backlog)); !bound) [[unlikely]]
    {
        throw std::system_error(bound.error(), "bind failed");
    }
}

TcpAcceptService::~TcpAcceptService() noexcept = default;

NBIO::Async::Task<NBIO::Runtime, 
    Utility::expected<std::pair<std::shared_ptr<TcpSessionService>, NBIO::Net::SocketAddress>, std::error_code>>
TcpAcceptService::accept() {
    auto accepted = co_await channel_.accept();
    if (!accepted) [[unlikely]] {
        co_return Utility::unexpected<std::error_code>(accepted.error());
    }
    auto [connector, peer] = std::move(*accepted);
    // The connector was made by the listener and is given up here: from this point the
    // session is what owns the connection, and the accepting side is ready for the
    // next one.
    co_return std::make_pair(std::make_shared<TcpSessionService>(std::move(connector)), std::move(peer));
}
}  // namespace NBIO::Net




