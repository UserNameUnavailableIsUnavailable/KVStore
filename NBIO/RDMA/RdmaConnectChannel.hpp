#pragma once
#if defined(__linux__)

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/RDMA/RdmaConnector.hpp>
#include <NBIO/RDMA/RdmaResourceManager.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/RDMA/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <memory>
#include <utility>

#include <NBIO/Core/Channel.hpp>
#include "RdmaSessionService.hpp"

namespace NBIO::RDMA {
class RdmaConnectChannel final : public NBIO::Core::Channel<RdmaConnectChannel> {
   public:
    using Handle = int;
    using Payload = detail::PollPayload<RdmaConnectChannel>;

    struct PendingConnect {
        std::shared_ptr<RdmaSessionService> session{};
        std::string error;
    };

    // The connection to establish: connect() finishes it and hands back a session
    // that owns it.
    RdmaConnectChannel(NBIO::RDMA::RdmaConnector& connector, NBIO::Core::Multiplexer& multiplexer,
                       NBIO::Async::Scheduler& scheduler);
    ~RdmaConnectChannel() noexcept;

    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::shared_ptr<RdmaSessionService>, std::string>> connect(
        NBIO::Net::SocketAddress peer);

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& submit();
    void complete();

    void park(NBIO::Async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    PendingConnect& job() noexcept { return job_; }

    const PendingConnect& job() const noexcept { return job_; }

    NBIO::RDMA::RdmaConnector& connector() noexcept { return connector_; }

   private:
    NBIO::RDMA::RdmaConnector& connector_;
    NBIO::Net::SocketAddress peer_{};
    PendingConnect job_{};
    NBIO::Async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace NBIO::RDMA

#endif  // defined(__linux__)
