#pragma once
#if defined(__linux__)

#include <nbio/async/Coroutine.hpp>
#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/net/RdmaConnector.hpp>
#include <nbio/net/RdmaResourceManager.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/Payload.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <memory>
#include <utility>

#include <nbio/core/Channel.hpp>
#include "RdmaSessionService.hpp"

namespace nbio::net {
class RdmaConnectChannel final : public nbio::core::Channel<RdmaConnectChannel> {
   public:
    using Handle = int;
    using Payload = detail::PollPayload<RdmaConnectChannel>;

    struct PendingConnect {
        std::shared_ptr<RdmaSessionService> session{};
        std::string error;
    };

    // The connection to establish: connect() finishes it and hands back a session
    // that owns it.
    RdmaConnectChannel(nbio::net::RdmaConnector& connector, nbio::core::Multiplexer& multiplexer,
                       nbio::async::Scheduler& scheduler);
    ~RdmaConnectChannel() noexcept;

    nbio::async::Task<nbio::runtime, nbio::utility::expected<std::shared_ptr<RdmaSessionService>, std::string>> Connect(
        nbio::net::Address peer);

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& Submit();
    void Complete();

    void Park(nbio::async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    PendingConnect& job() noexcept { return job_; }

    const PendingConnect& job() const noexcept { return job_; }

    nbio::net::RdmaConnector& connector() noexcept { return connector_; }

   private:
    nbio::net::RdmaConnector& connector_;
    nbio::net::Address peer_{};
    PendingConnect job_{};
    nbio::async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace nbio::net

#endif  // defined(__linux__)
