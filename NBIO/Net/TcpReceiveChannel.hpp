#pragma once

#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/Net/TcpConnector.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Net/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <deque>
#include <span>
#include <system_error>

namespace NBIO::Net {
class ReceiveAwaiter;

class TcpReceiveChannel final : public NBIO::Core::Channel<TcpReceiveChannel> {
   public:
    using Payload = detail::MessagePayload<TcpReceiveChannel>;

    explicit TcpReceiveChannel(NBIO::Net::TcpConnector& connector, NBIO::Core::Multiplexer& multiplexer,
                               NBIO::Async::Scheduler& scheduler);
    ~TcpReceiveChannel() noexcept;

    NBIO::Async::Task<NBIO::Runtime, Utility::expected<std::size_t, std::error_code>> receive(std::span<char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload& submit();
    void complete();

    Net::TcpConnector& connector() noexcept { return connector_; }
    const Net::TcpConnector& connector() const noexcept { return connector_; }

   private:
    friend class ReceiveAwaiter;

    // Queues the receive and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, Net::Transmission* transmission);

    NBIO::Net::TcpConnector& connector_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace NBIO::Net




