#pragma once

#include "Channel.hpp"
#include "Runtime.hpp"
#include "Multiplexer.hpp"
#include "Payload.hpp"

#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/TcpConnector.hpp>

#include <deque>
#include <span>
#include <system_error>

namespace Foundation::NBIO
{
class SendAwaiter;

class TcpSendChannel final : public Foundation::NBIO::Channel
{
  public:
    explicit TcpSendChannel(Foundation::Core::TcpConnector &connector, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~TcpSendChannel() noexcept;

    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> send(std::span<const char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload &submit();
    void complete();

  private:
    friend class SendAwaiter;

    void prepare(Foundation::Async::Coroutine waiter, Core::Transmission *transmission);

    Foundation::Core::TcpConnector &connector_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{SendPayload{}};
};
} // namespace Foundation::NBIO
