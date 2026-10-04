#include "RdmaConnectChannel.hpp"

#include <nbio/async/Coroutine.hpp>
#include <cstdint>
#include <utility>

namespace nbio::net {
namespace detail {
class RdmaConnectAwaiter {
   public:
    RdmaConnectAwaiter(RdmaConnectChannel& channel, nbio::net::Address peer)
        : channel_(channel), peer_(std::move(peer)) {}

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType>) {
        channel_.job().session.reset();
        channel_.job().error = {};

        if (auto connected = channel_.connector().Connect(peer_); connected) {
            // The session owns the connection from here on: the connector the caller
            // holds is moved from, and this channel never connects it again.
            channel_.job().session = std::make_shared<RdmaSessionService>(std::move(channel_.connector()),
                                                                          channel_.multiplexer(), channel_.scheduler());
        } else {
            channel_.job().error = connected.error();
        }
        return false;
    }

    nbio::utility::expected<std::shared_ptr<RdmaSessionService>, std::string> await_resume() {
        auto& job = channel_.job();
        if (job.session) {
            return std::move(job.session);
        } else {
            return nbio::utility::unexpected(std::move(job.error));
        }
    }

   private:
    RdmaConnectChannel& channel_;
    nbio::net::Address peer_;
};
}  // namespace detail

RdmaConnectChannel::RdmaConnectChannel(nbio::net::RdmaConnector& connector, nbio::core::Multiplexer& multiplexer,
                                       nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<RdmaConnectChannel>(nbio::core::ChannelType::kRdmaConnect,
                                                    static_cast<std::uintptr_t>(connector.cm_handle()), multiplexer,
                                                    scheduler),
      connector_(connector) {
    // What there is to watch is the CM channel the connection was created with: the
    // handshake it is about to run reports there. The connect itself is made
    // synchronously in the awaiter, which never suspends.
}

RdmaConnectChannel::~RdmaConnectChannel() noexcept { multiplexer_.DeleteChannel(this); }

nbio::async::Task<nbio::runtime, nbio::utility::expected<std::shared_ptr<RdmaSessionService>, std::string>> RdmaConnectChannel::Connect(
    nbio::net::Address peer) {
    co_return co_await detail::RdmaConnectAwaiter{*this, std::move(peer)};
}

RdmaConnectChannel::Payload& RdmaConnectChannel::Submit() { return payload_; }

void RdmaConnectChannel::Complete() {
    auto& payload = payload_;
    payload.release_poll();

    if (!waiter_) [[unlikely]] {
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.Submit(std::move(waiter));
}
}  // namespace nbio::net
