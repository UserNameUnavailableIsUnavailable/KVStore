#include "RdmaConnectChannel.hpp"

#include <NBIO/Async/Coroutine.hpp>
#include <cstdint>
#include <utility>

namespace NBIO::RDMA {
namespace detail {
class RdmaConnectAwaiter {
   public:
    RdmaConnectAwaiter(RdmaConnectChannel& channel, NBIO::Net::SocketAddress peer)
        : channel_(channel), peer_(std::move(peer)) {}

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType>) {
        channel_.job().session.reset();
        channel_.job().error = {};

        if (auto connected = channel_.connector().connect(peer_); connected) {
            // The session owns the connection from here on: the connector the caller
            // holds is moved from, and this channel never connects it again.
            channel_.job().session = std::make_shared<RdmaSessionService>(std::move(channel_.connector()),
                                                                          channel_.multiplexer(), channel_.scheduler());
        } else {
            channel_.job().error = connected.error();
        }
        return false;
    }

    NBIO::Utility::expected<std::shared_ptr<RdmaSessionService>, std::string> await_resume() {
        auto& job = channel_.job();
        if (job.session) {
            return std::move(job.session);
        } else {
            return NBIO::Utility::unexpected(std::move(job.error));
        }
    }

   private:
    RdmaConnectChannel& channel_;
    NBIO::Net::SocketAddress peer_;
};
}  // namespace detail

RdmaConnectChannel::RdmaConnectChannel(NBIO::RDMA::RdmaConnector& connector, NBIO::Core::Multiplexer& multiplexer,
                                       NBIO::Async::Scheduler& scheduler)
    : NBIO::Core::Channel<RdmaConnectChannel>(NBIO::Core::ChannelType::kRdmaConnect,
                                                    static_cast<std::uintptr_t>(connector.cm_handle()), multiplexer,
                                                    scheduler),
      connector_(connector) {
    // What there is to watch is the CM channel the connection was created with: the
    // handshake it is about to run reports there. The connect itself is made
    // synchronously in the awaiter, which never suspends.
}

RdmaConnectChannel::~RdmaConnectChannel() noexcept { multiplexer_.delete_channel(this); }

NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::shared_ptr<RdmaSessionService>, std::string>> RdmaConnectChannel::connect(
    NBIO::Net::SocketAddress peer) {
    co_return co_await detail::RdmaConnectAwaiter{*this, std::move(peer)};
}

RdmaConnectChannel::Payload& RdmaConnectChannel::submit() { return payload_; }

void RdmaConnectChannel::complete() {
    auto& payload = payload_;
    payload.release_poll();

    if (!waiter_) [[unlikely]] {
        return;
    }

    auto waiter = std::exchange(waiter_, {});
    scheduler_.submit(std::move(waiter));
}
}  // namespace NBIO::RDMA
