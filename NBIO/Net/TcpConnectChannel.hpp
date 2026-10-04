#pragma once

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Expected.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpConnector.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Net/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Core/Types.hpp>
#include <system_error>
#include <utility>

namespace NBIO::Net {
class ConnectAwaiter;
class TcpConnectChannel final : public NBIO::Core::Channel<TcpConnectChannel> {
   public:
    using Payload = detail::PollPayload<TcpConnectChannel>;

    TcpConnectChannel(NBIO::Net::TcpConnector connector, NBIO::Core::Multiplexer& multiplexer,
                      NBIO::Async::Scheduler& scheduler);

    ~TcpConnectChannel() noexcept;

    NBIO::Async::Task<NBIO::Runtime, Utility::expected<NBIO::Net::TcpConnector, std::error_code>> connect(
        const NBIO::Net::SocketAddress& target);

    NBIO::Async::Task<NBIO::Runtime, Utility::expected<NBIO::Net::TcpConnector, std::error_code>> connect(
        const NBIO::Net::SocketAddress& source, const NBIO::Net::SocketAddress& target);

    Payload& submit();
    void complete();

   private:
    friend class ConnectAwaiter;

    void park(Async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    NBIO::Net::TcpConnector connector_;
    Async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace NBIO::Net




