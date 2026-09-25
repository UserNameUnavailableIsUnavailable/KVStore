#include "TcpConnectService.hpp"

#include <Foundation/Core/TcpConnector.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/TcpConnectChannel.hpp>

#include <memory>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
namespace
{
// The connect body, as a plain coroutine whose channel is an ordinary local: the
// channel lives in this frame for the length of one connect, which is exactly as long
// as a connect channel is good for.
Foundation::NBIO::Task<Core::expected<std::shared_ptr<TcpSessionService>, std::error_code>> ConnectOn(
    const Foundation::Core::SocketAddress *source, const Foundation::Core::SocketAddress &target,
    Foundation::Core::SocketAddress::Family family)
{
    TcpConnectChannel channel{Foundation::Core::TcpConnector{family}, Engine::multiplexer(), Engine::scheduler()};
    auto connected = source != nullptr ? co_await channel.connect(*source, target) : co_await channel.connect(target);
    if (!connected) [[unlikely]]
    {
        co_return Core::unexpected<std::error_code>(connected.error());
    }
    // The service is spent and the session is what is left: the connection goes
    // straight into it, and the channel that made it is destroyed with this frame.
    co_return std::make_shared<TcpSessionService>(std::move(*connected));
}
} // namespace

TcpConnectService::TcpConnectService(Foundation::Core::SocketAddress::Family family) : family_(family)
{
}

Foundation::NBIO::Task<Core::expected<std::shared_ptr<TcpSessionService>, std::error_code>> TcpConnectService::connect(
    const Foundation::Core::SocketAddress &target)
{
    return ConnectOn(nullptr, target, family_);
}

Foundation::NBIO::Task<Core::expected<std::shared_ptr<TcpSessionService>, std::error_code>> TcpConnectService::connect(
    const Foundation::Core::SocketAddress &source, const Foundation::Core::SocketAddress &target)
{
    return ConnectOn(&source, target, family_);
}
} // namespace Foundation::NBIO
