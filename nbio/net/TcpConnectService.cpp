#include "TcpConnectService.hpp"

#include <nbio/net/TcpConnector.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/net/TcpConnectChannel.hpp>
#include <memory>
#include <system_error>
#include <utility>

namespace nbio::net {
namespace {
// The connect body, as a plain coroutine whose channel is an ordinary local: the
// channel lives in this frame for the length of one connect, which is exactly as long
// as a connect channel is good for.
nbio::async::Task<nbio::runtime, utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> ConnectOn(
    const nbio::net::Address* source, const nbio::net::Address& target,
    nbio::net::Address::Family family) {
    TcpConnectChannel channel{nbio::net::TcpConnector{family}, nbio::runtime::multiplexer(), nbio::runtime::scheduler()};
    auto connected = source != nullptr ? co_await channel.Connect(*source, target) : co_await channel.Connect(target);
    if (!connected) [[unlikely]] {
        co_return utility::unexpected<std::error_code>(connected.error());
    }
    // The service is spent and the session is what is left: the connection goes
    // straight into it, and the channel that made it is destroyed with this frame.
    co_return std::make_shared<TcpSessionService>(std::move(*connected));
}
}  // namespace

TcpConnectService::TcpConnectService(nbio::net::Address::Family family) : family_(family) {}

nbio::async::Task<nbio::runtime, utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> TcpConnectService::Connect(
    const nbio::net::Address& target) {
    return ConnectOn(nullptr, target, family_);
}

nbio::async::Task<nbio::runtime, utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> TcpConnectService::Connect(
    const nbio::net::Address& source, const nbio::net::Address& target) {
    return ConnectOn(&source, target, family_);
}
}  // namespace nbio::net




