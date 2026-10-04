#pragma once

#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/Net/TcpConnector.hpp>
#include <deque>
#include <span>
#include <system_error>

#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Net/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>

namespace NBIO::Net {
class SendAwaiter;

class TcpSendChannel final : public NBIO::Core::Channel<TcpSendChannel> {
   public:
    using Payload = detail::MessagePayload<TcpSendChannel>;

    explicit TcpSendChannel(NBIO::Net::TcpConnector& connector, NBIO::Core::Multiplexer& multiplexer,
                            NBIO::Async::Scheduler& scheduler);
    ~TcpSendChannel() noexcept;

    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> send(std::span<const char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload& submit();
    void complete();

    Net::TcpConnector& connector() noexcept { return connector_; }

    const Net::TcpConnector& connector() const noexcept { return connector_; }

   private:
    friend class SendAwaiter;

    void prepare(NBIO::Async::Coroutine waiter, Net::Transmission* transmission);

    NBIO::Net::TcpConnector& connector_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace NBIO::Net




