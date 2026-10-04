#include "TcpConnectService.hpp"

#include <NBIO/Net/TcpConnector.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Net/TcpConnectChannel.hpp>
#include <memory>
#include <system_error>
#include <utility>

namespace NBIO::Net {
namespace {
// The connect body, as a plain coroutine whose channel is an ordinary local: the
// channel lives in this frame for the length of one connect, which is exactly as long
// as a connect channel is good for.
NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> ConnectOn(
    const NBIO::Net::SocketAddress* source, const NBIO::Net::SocketAddress& target,
    NBIO::Net::SocketAddress::Family family) {
    TcpConnectChannel channel{NBIO::Net::TcpConnector{family}, NBIO::Runtime::multiplexer(), NBIO::Runtime::scheduler()};
    auto connected = source != nullptr ? co_await channel.connect(*source, target) : co_await channel.connect(target);
    if (!connected) [[unlikely]] {
        co_return Utility::unexpected<std::error_code>(connected.error());
    }
    // The service is spent and the session is what is left: the connection goes
    // straight into it, and the channel that made it is destroyed with this frame.
    co_return std::make_shared<TcpSessionService>(std::move(*connected));
}
}  // namespace

TcpConnectService::TcpConnectService(NBIO::Net::SocketAddress::Family family) : family_(family) {}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> TcpConnectService::connect(
    const NBIO::Net::SocketAddress& target) {
    return ConnectOn(nullptr, target, family_);
}

NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::shared_ptr<TcpSessionService>, std::error_code>> TcpConnectService::connect(
    const NBIO::Net::SocketAddress& source, const NBIO::Net::SocketAddress& target) {
    return ConnectOn(&source, target, family_);
}
}  // namespace NBIO::Net




