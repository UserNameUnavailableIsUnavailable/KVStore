#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/RdmaAcceptor.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"
#include "RdmaSession.hpp"
#include <Foundation/NBIO/Payload.hpp>

#include <memory>
#include <string>
#include <utility>

namespace Foundation::NBIO
{
class RdmaAcceptChannel final : public Channel
{
  public:
    using Handle = int;

    struct PendingAccept
    {
        std::shared_ptr<RdmaSession> session{};
        // Why no connection can be admitted any more, empty while none has
        // failed.
        std::string error{};
    };

    RdmaAcceptChannel(Foundation::Core::RdmaAcceptor &acceptor, Multiplexer &multiplexer,
                       Foundation::Async::Scheduler &scheduler);
    ~RdmaAcceptChannel() noexcept;

    Task<Core::expected<std::shared_ptr<RdmaSession>, std::string>> accept();

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload &submit();
    void complete();

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    PendingAccept &job() noexcept
    {
        return job_;
    }

    const PendingAccept &job() const noexcept
    {
        return job_;
    }

    Foundation::Core::RdmaAcceptor &acceptor() noexcept
    {
        return acceptor_;
    }

  private:
    Foundation::Core::RdmaAcceptor &acceptor_;
    PendingAccept job_{};
    Foundation::Async::Coroutine waiter_{};
    Payload payload_{RdmaAcceptPayload{}};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
