#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"
#include <Foundation/NBIO/Payload.hpp>

#include <optional>
#include <span>
#include <string>
#include <utility>

namespace Foundation::NBIO
{
class RdmaReceiveChannel final : public Channel
{
  public:
    using Handle = int;

        struct PendingReceive
        {
                std::optional<std::span<char>> chunk{};
                // Why no chunk can arrive any more, empty while none has failed.
                std::string error{};
        };

    ~RdmaReceiveChannel() noexcept;

    RdmaReceiveChannel(Foundation::Core::RdmaConnector &connection, Multiplexer &multiplexer,
                        Foundation::Async::Scheduler &scheduler);

    // Waits for a chunk the peer filled. An empty answer is not a failure: the
    // peer simply has not sent anything yet.
    Foundation::NBIO::Task<Core::expected<std::optional<std::span<char>>, std::string>> receive();

    // The same, for a caller that is willing to carry on without one.
    Foundation::NBIO::Task<Core::expected<std::optional<std::span<char>>, std::string>> try_receive();

    // Hands a received chunk back for the next message.
    Core::expected<void, std::string> release(std::span<char> chunk) noexcept;

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

    PendingReceive &job() noexcept
    {
        return job_;
    }

    const PendingReceive &job() const noexcept
    {
        return job_;
    }

    private:
    Foundation::Core::RdmaConnector &connection_;
    PendingReceive job_{};
    Foundation::Async::Coroutine waiter_{};
    Payload payload_{RdmaReceivePayload{}};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
