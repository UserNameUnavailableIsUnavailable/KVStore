#pragma once

#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/TcpConnector.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <deque>
#include <span>
#include <system_error>

namespace Foundation::NBIO {
class ReceiveAwaiter;

class TcpReceiveChannel final : public Foundation::NBIO::Channel<TcpReceiveChannel> {
   public:
    using Payload = detail::MessagePayload<TcpReceiveChannel>;

    explicit TcpReceiveChannel(Foundation::Core::TcpConnector& connector, Foundation::NBIO::Multiplexer& multiplexer,
                               Foundation::Async::Scheduler& scheduler);
    ~TcpReceiveChannel() noexcept;

    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> receive(std::span<char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload& submit();
    void complete();

    Core::TcpConnector& connector() noexcept { return connector_; }
    const Core::TcpConnector& connector() const noexcept { return connector_; }

   private:
    friend class ReceiveAwaiter;

    // Queues the receive and arms the channel: this is the suspension point, and
    // being armed is what tells the backend to look at the channel.
    void prepare(Async::Coroutine waiter, Core::Transmission* transmission);

    Foundation::Core::TcpConnector& connector_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{};
};
}  // namespace Foundation::NBIO
