#pragma once
#if defined(__linux__)

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/RDMA/RdmaAcceptor.hpp>
#include <NBIO/RDMA/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <memory>
#include <string>
#include <utility>

#include <NBIO/Core/Channel.hpp>
#include "RdmaSessionService.hpp"

namespace NBIO::RDMA {
class RdmaAcceptChannel final : public NBIO::Core::Channel<RdmaAcceptChannel> {
   public:
    using Handle = int;
    using Payload = detail::PollPayload<RdmaAcceptChannel>;

    struct PendingAccept {
        std::shared_ptr<RdmaSessionService> session{};
        // Why no connection can be admitted any more, empty while none has
        // failed.
        std::string error{};
    };

    RdmaAcceptChannel(NBIO::RDMA::RdmaAcceptor& acceptor, NBIO::Core::Multiplexer& multiplexer,
                      NBIO::Async::Scheduler& scheduler);
    ~RdmaAcceptChannel() noexcept;

    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::shared_ptr<RdmaSessionService>, std::string>> accept();

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& submit();
    void complete();

    void park(NBIO::Async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    PendingAccept& job() noexcept { return job_; }

    const PendingAccept& job() const noexcept { return job_; }

    NBIO::RDMA::RdmaAcceptor& acceptor() noexcept { return acceptor_; }

   private:
    NBIO::RDMA::RdmaAcceptor& acceptor_;
    PendingAccept job_{};
    NBIO::Async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace NBIO::RDMA

#endif  // defined(__linux__)
