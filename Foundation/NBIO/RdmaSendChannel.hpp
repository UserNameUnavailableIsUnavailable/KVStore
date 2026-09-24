#pragma once
#if defined(__linux__)

#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include "Channel.hpp"
#include <Foundation/NBIO/Payload.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <span>
#include <string>
#include <utility>

namespace Foundation::NBIO
{
class RdmaSendChannel final : public Channel
{
  public:
    using Handle = int;

    struct PendingSend
    {
        // How many sends may still be in flight for the parked poll to be met.
        std::size_t target{0};
        // Completions reaped since the poll began.
        std::size_t completions{0};
        // Why the stream stopped completing anything, empty while it has not.
        std::string error{};
    };

    RdmaSendChannel(Foundation::Core::RdmaConnector &connection, Multiplexer &multiplexer,
             Foundation::Async::Scheduler &scheduler);
    ~RdmaSendChannel() noexcept;

    // A chunk to fill. Several can be held at once, so the way to use this is to
    // take as many as the stream will give, fill them, and send them -- the
    // device carries them in parallel instead of one per round trip. An empty
    // answer means every chunk is already in flight.
    Core::expected<std::optional<std::span<char>>, std::string> acquire() noexcept
    {
        return connection_.acquire();
    }

    // Hands one acquired chunk to the device. Returns immediately: the chunk
    // belongs to the device until a completion retires it, which poll() reports.
    Core::expected<void, std::string> send(std::span<char> chunk, std::size_t length) noexcept
    {
        return connection_.send(chunk, length);
    }

    // Waits until at least `count` of the sends in flight when it was called have
    // completed, or -- for the default -- until all of them have, after which
    // every chunk has been handed back. Answers how many completed while it
    // waited, which is also how many chunks became available, or why the stream
    // gave up completing them.
    Foundation::NBIO::Task<Core::expected<std::size_t, std::string>> poll(std::size_t count = 0);

    // Sends posted and not yet reaped.
    std::size_t outstanding() const noexcept
    {
        return connection_.outstanding_sends();
    }

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload &submit();
    void complete();

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    Foundation::Core::RdmaConnector &connection() noexcept
    {
        return connection_;
    }

    PendingSend &job() noexcept
    {
      return job_;
    }

    const PendingSend &job() const noexcept
    {
      return job_;
    }

    // Completions reaped so far, which a poll reads to answer with a difference.
    std::size_t &completed() noexcept
    {
        return completed_;
    }

  private:
    Foundation::Core::RdmaConnector &connection_;
    PendingSend job_{};
    std::size_t completed_{0};
    Foundation::Async::Coroutine waiter_{};
    Payload payload_{RdmaSendPayload{}};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
