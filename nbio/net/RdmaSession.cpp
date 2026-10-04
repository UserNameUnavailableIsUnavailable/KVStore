#include "RdmaSession.hpp"

namespace nbio::net {
RdmaSessionService::RdmaSessionService(nbio::net::RdmaConnector connection, nbio::core::Multiplexer& multiplexer,
                                       nbio::async::Scheduler& scheduler)
    : connection_(std::move(connection)),
      send_channel_(connection_, multiplexer, scheduler),
      receive_channel_(connection_, multiplexer, scheduler) {}

nbio::utility::expected<void, std::string> RdmaSessionService::send(std::span<char> chunk, std::size_t length) noexcept {
    return send_channel_.send(chunk, length);
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<std::size_t, std::string>> RdmaSessionService::PollSend(std::size_t count) {
    co_return co_await send_channel_.poll(count);
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<std::optional<std::span<char>>, std::string>> RdmaSessionService::receive() {
    co_return co_await receive_channel_.receive();
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<std::optional<std::span<char>>, std::string>> RdmaSessionService::try_receive() {
    co_return co_await receive_channel_.try_receive();
}

nbio::utility::expected<void, std::string> RdmaSessionService::release(std::span<char> chunk) noexcept {
    return receive_channel_.release(chunk);
}

nbio::net::RdmaConnector& RdmaSessionService::connection() noexcept { return connection_; }

const nbio::net::RdmaConnector& RdmaSessionService::connection() const noexcept { return connection_; }

RdmaSendChannel& RdmaSessionService::send_channel() noexcept { return send_channel_; }

const RdmaSendChannel& RdmaSessionService::send_channel() const noexcept { return send_channel_; }

RdmaReceiveChannel& RdmaSessionService::receive_channel() noexcept { return receive_channel_; }

const RdmaReceiveChannel& RdmaSessionService::receive_channel() const noexcept { return receive_channel_; }
}  // namespace nbio::net
