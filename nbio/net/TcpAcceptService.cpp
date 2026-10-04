#include "TcpAcceptService.hpp"

#include <nbio/runtime/Runtime.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace nbio::net {
TcpAcceptService::TcpAcceptService(const nbio::net::Address& address, int backlog)
    : acceptor_(address.family()), channel_(acceptor_, nbio::runtime::multiplexer(), nbio::runtime::scheduler()) {
    // A listener that cannot be restarted while the socket it replaced is still in
    // TIME_WAIT is not much of a listener, and nothing else can set this: the socket
    // belongs to the acceptor.
    if (auto reuse = acceptor_.ReuseAddress(true); !reuse) [[unlikely]]
    {
        throw std::system_error(reuse.error(), "setsockopt(SO_REUSEADDR) failed");
    }
    if (auto bound = acceptor_.Bind(address, static_cast<std::size_t>(backlog)); !bound) [[unlikely]]
    {
        throw std::system_error(bound.error(), "bind failed");
    }
}

TcpAcceptService::~TcpAcceptService() noexcept = default;

nbio::async::Task<nbio::runtime, 
    utility::expected<std::pair<std::shared_ptr<TcpSessionService>, nbio::net::Address>, std::error_code>>
TcpAcceptService::Accept() {
    auto accepted = co_await channel_.Accept();
    if (!accepted) [[unlikely]] {
        co_return utility::unexpected<std::error_code>(accepted.error());
    }
    auto [connector, peer] = std::move(*accepted);
    // The connector was made by the listener and is given up here: from this point the
    // session is what owns the connection, and the accepting side is ready for the
    // next one.
    co_return std::make_pair(std::make_shared<TcpSessionService>(std::move(connector)), std::move(peer));
}
}  // namespace nbio::net




