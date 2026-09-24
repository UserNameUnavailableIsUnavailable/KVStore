#pragma once

#include "Channel.hpp"
#include "Runtime.hpp"
#include "Multiplexer.hpp"
#include "Payload.hpp"

#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/TcpSocket.hpp>

#include <deque>
#include <span>
#include <system_error>

namespace Foundation::NBIO
{
class SendAwaiter;

class TcpSendChannel final : public Foundation::NBIO::Channel
{
  public:
    explicit TcpSendChannel(Foundation::Core::TcpSocket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer);
    ~TcpSendChannel() noexcept;

    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> send(std::span<const char> buffer);

    // The operation the backend performs lives in the payload; the backend fills
    // the transmissions and asks the channel to reap them.
    Payload &submit();
    void complete();

    Foundation::Core::TcpSocket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::TcpSocket &socket() const noexcept
    {
        return socket_;
    }

  private:
    friend class SendAwaiter;

    void prepare(Foundation::Async::Coroutine waiter, Core::Transmission *transmission);

    Foundation::Core::TcpSocket &socket_;
    // One waiter per transmission, in queue order.
    std::deque<Async::Coroutine> waiters_;
    Payload payload_{SendPayload{}};
};
} // namespace Foundation::NBIO
