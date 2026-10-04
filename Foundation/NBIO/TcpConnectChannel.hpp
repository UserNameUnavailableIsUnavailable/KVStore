#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/TcpConnector.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <system_error>
#include <utility>

namespace Foundation::NBIO {
class ConnectAwaiter;
class TcpConnectChannel final : public Foundation::NBIO::Channel<TcpConnectChannel> {
   public:
    using Payload = detail::PollPayload<TcpConnectChannel>;

    TcpConnectChannel(Foundation::Core::TcpConnector connector, Foundation::NBIO::Multiplexer& multiplexer,
                      Foundation::Async::Scheduler& scheduler);

    ~TcpConnectChannel() noexcept;

    Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> connect(
        const Foundation::Core::SocketAddress& target);

    Foundation::NBIO::Task<Core::expected<Foundation::Core::TcpConnector, std::error_code>> connect(
        const Foundation::Core::SocketAddress& source, const Foundation::Core::SocketAddress& target);

    Payload& submit();
    void complete();

   private:
    friend class ConnectAwaiter;

    void park(Async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    Foundation::Core::TcpConnector connector_;
    Async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace Foundation::NBIO
