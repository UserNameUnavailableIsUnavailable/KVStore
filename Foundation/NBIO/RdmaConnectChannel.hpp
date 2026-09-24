#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/Core/RdmaResourceManager.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"
#include "RdmaSession.hpp"
#include <Foundation/NBIO/Payload.hpp>

#include <memory>
#include <utility>

namespace Foundation::NBIO
{
class RdmaConnectChannel final : public Channel
{
  public:
    using Handle = int;

    struct PendingConnect
    {
        std::shared_ptr<RdmaSession> session{};
        std::string error;
    };

    // The connection to establish: connect() finishes it and hands back a session
    // that owns it.
    RdmaConnectChannel(Foundation::Core::RdmaConnector &connector, Multiplexer &multiplexer,
                        Foundation::Async::Scheduler &scheduler);
    ~RdmaConnectChannel() noexcept;

    Task<Core::expected<std::shared_ptr<RdmaSession>, std::string>> connect(Foundation::Core::SocketAddress peer);

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload &submit();
    void complete();

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    PendingConnect &job() noexcept
    {
        return job_;
    }

    const PendingConnect &job() const noexcept
    {
        return job_;
    }

    Foundation::Core::RdmaConnector &connector() noexcept
    {
        return connector_;
    }

  private:
    Foundation::Core::RdmaConnector &connector_;
    Foundation::Core::SocketAddress peer_{};
    PendingConnect job_{};
    Foundation::Async::Coroutine waiter_{};
    Payload payload_{RdmaConnectPayload{}};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
