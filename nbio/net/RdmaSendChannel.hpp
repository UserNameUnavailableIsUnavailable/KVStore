#pragma once
#if defined(__linux__)

#include <nbio/async/Coroutine.hpp>
#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/net/RdmaConnector.hpp>
#include <nbio/net/Payload.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <span>
#include <string>
#include <utility>

#include <nbio/core/Channel.hpp>

namespace nbio::net {
class RdmaSendChannel final : public nbio::core::Channel<RdmaSendChannel> {
   public:
    using Handle = int;
    using Payload = detail::PollPayload<RdmaSendChannel>;

    struct PendingSend {
        // How many sends may still be in flight for the Parked poll to be met.
        std::size_t target{0};
        // Completions reaped since the poll began.
        std::size_t completions{0};
        // Why the stream stopped completing anything, empty while it has not.
        std::string error{};
    };

    RdmaSendChannel(nbio::net::RdmaConnector& connection, nbio::core::Multiplexer& multiplexer,
                    nbio::async::Scheduler& scheduler);
    ~RdmaSendChannel() noexcept;

    // A chunk to fill. Several can be held at once, so the way to use this is to
    // take as many as the stream will give, fill them, and send them -- the
    // device carries them in parallel instead of one per round trip. An empty
    // answer means every chunk is already in flight.
    nbio::utility::expected<std::optional<std::span<char>>, std::string> Acquire() noexcept { return connection_.acquire(); }

    // Hands one acquired chunk to the device. Returns immediately: the chunk
    // belongs to the device until a completion retires it, which poll() reports.
    nbio::utility::expected<void, std::string> Send(std::span<char> chunk, std::size_t length) noexcept {
        return connection_.send(chunk, length);
    }

    // Waits until at least `count` of the sends in flight when it was called have
    // completed, or -- for the default -- until all of them have, after which
    // every chunk has been handed back. Answers how many completed while it
    // waited, which is also how many chunks became available, or why the stream
    // gave up completing them.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<std::size_t, std::string>> Poll(std::size_t count = 0);

    // Sends posted and not yet reaped.
    std::size_t outstanding() const noexcept { return connection_.outstanding_sends(); }

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& Submit();
    void Complete();

    void Park(nbio::async::Coroutine waiter) noexcept { waiter_ = std::move(waiter); }

    nbio::net::RdmaConnector& connection() noexcept { return connection_; }

    PendingSend& job() noexcept { return job_; }

    const PendingSend& job() const noexcept { return job_; }

    // Completions reaped so far, which a poll reads to answer with a difference.
    std::size_t& completed() noexcept { return completed_; }

   private:
    nbio::net::RdmaConnector& connection_;
    PendingSend job_{};
    std::size_t completed_{0};
    nbio::async::Coroutine waiter_{};
    Payload payload_{};
};
}  // namespace nbio::net

#endif  // defined(__linux__)
