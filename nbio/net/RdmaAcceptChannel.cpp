#include "RdmaAcceptChannel.hpp"

#include <nbio/async/Coroutine.hpp>
#include <stdexcept>
#include <string>
#include <utility>

namespace nbio::net {
namespace detail {
class RdmaAcceptAwaiter {
   public:
    explicit RdmaAcceptAwaiter(RdmaAcceptChannel& channel) : channel_(channel) {}

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> handle) {
        channel_.job().session.reset();
        channel_.job().error = {};
        channel_.Park(nbio::async::Coroutine::FromHandle(handle));
        channel_.Arm();
        return true;
    }

    nbio::utility::expected<std::shared_ptr<RdmaSessionService>, std::string> await_resume() {
        auto& job = channel_.job();
        if (job.session) {
            return std::move(job.session);
        }
        return nbio::utility::unexpected(std::exchange(job.error, std::string{}));
    }

   private:
    RdmaAcceptChannel& channel_;
};
}  // namespace detail

RdmaAcceptChannel::RdmaAcceptChannel(nbio::net::RdmaAcceptor& acceptor, nbio::core::Multiplexer& multiplexer,
                                     nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<RdmaAcceptChannel>(nbio::core::ChannelType::kRdmaAccept,
                                                   static_cast<std::uintptr_t>(acceptor.native_handle()), multiplexer,
                                                   scheduler),
      acceptor_(acceptor) {
    // The acceptor has to be polled rather than waited on, and this is the one
    // failure the channel cannot report through a job -- there is no waiter to
    // report it to yet. A constructor is the one place still allowed to throw.
    if (auto armed = acceptor_.NonBlocking(true); !armed) [[unlikely]]
    {
        throw std::runtime_error("Failed to make the rdma acceptor non-blocking: " + armed.error());
    }
    // Registered on the first arm(): nothing to watch until a wait queues.
}

RdmaAcceptChannel::~RdmaAcceptChannel() noexcept { multiplexer_.DeleteChannel(this); }

nbio::async::Task<nbio::runtime, nbio::utility::expected<std::shared_ptr<RdmaSessionService>, std::string>> RdmaAcceptChannel::accept() {
    co_return co_await detail::RdmaAcceptAwaiter{*this};
}

RdmaAcceptChannel::Payload& RdmaAcceptChannel::Submit() { return payload_; }

void RdmaAcceptChannel::Complete() {
    auto& payload = payload_;
    payload.release_poll();

    if (!waiter_) [[unlikely]] {
        return;
    }

    auto accepted = acceptor_.Accept();
    if (!accepted) [[unlikely]] {
        job().session.reset();
        job().error = accepted.error();
    } else if (*accepted) {
        // Building the session allocates its two channels, which is the one step
        // left that can throw. Failing to build it belongs to this accept, not to
        // whoever is waiting on the engine.
        try {
            job().session = std::make_shared<RdmaSessionService>(std::move(**accepted), multiplexer_, scheduler_);
            job().error = {};
        } catch (const std::exception& failure) {
            job().session.reset();
            job().error = std::string{failure.what()};
        }
    } else {
        // Nothing has asked to be admitted yet -- or the event was about a
        // connection that is already established. Either way the waiter stays
        // Parked and the channel keeps watching.
        job().session.reset();
        job().error = {};
        Arm();
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.Submit(std::move(waiter));
}
}  // namespace nbio::net
