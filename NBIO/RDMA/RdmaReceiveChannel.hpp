#pragma once
#if defined(__linux__)

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/RDMA/RdmaConnector.hpp>
#include <NBIO/RDMA/Payload.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <optional>
#include <span>
#include <string>
#include <utility>

#include <NBIO/Core/Channel.hpp>

namespace NBIO::RDMA {
class RdmaReceiveChannel final : public NBIO::Core::Channel<RdmaReceiveChannel> {
   public:
    using Handle = int;
    using Payload = detail::PollPayload<RdmaReceiveChannel>;

    struct PendingReceive {
        std::optional<std::span<char>> chunk{};
        // Why no chunk can arrive any more, empty while none has failed.
        std::string error{};
    };

    ~RdmaReceiveChannel() noexcept;

    RdmaReceiveChannel(NBIO::RDMA::RdmaConnector& connection, NBIO::Core::Multiplexer& multiplexer,
                       NBIO::Async::Scheduler& scheduler);

    // Waits for a chunk the peer filled. An empty answer is not a failure: the
    // peer simply has not sent anything yet.
    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::optional<std::span<char>>, std::string>> receive();

    // The same, for a caller that is willing to carry on without one.
    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::optional<std::span<char>>, std::string>> try_receive();

    // Hands a received chunk back for the next message.
    NBIO::Utility::expected<void, std::string> release(std::span<char> chunk) noexcept;

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& submit();
    void complete();

    void park(NBIO::Async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    NBIO::RDMA::RdmaConnector& connection() noexcept { return connection_; }

    PendingReceive& job() noexcept { return job_; }

    const PendingReceive& job() const noexcept { return job_; }

   private:
    NBIO::RDMA::RdmaConnector& connection_;
    PendingReceive job_{};
    NBIO::Async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace NBIO::RDMA

#endif  // defined(__linux__)
