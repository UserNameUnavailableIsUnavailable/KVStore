#pragma once
#if defined(__linux__)

#include <nbio/net/RdmaConnector.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <optional>
#include <span>
#include <string>

#include "RdmaReceiveChannel.hpp"
#include "RdmaSendChannel.hpp"

namespace nbio::net {
class Multiplexer;

class RdmaSessionService final {
   public:
    RdmaSessionService(nbio::net::RdmaConnector connection, nbio::core::Multiplexer& multiplexer,
                       nbio::async::Scheduler& scheduler);

    RdmaSessionService(const RdmaSessionService&) = delete;
    RdmaSessionService& operator=(const RdmaSessionService&) = delete;
    RdmaSessionService(RdmaSessionService&&) = delete;
    RdmaSessionService& operator=(RdmaSessionService&&) = delete;
    ~RdmaSessionService() noexcept = default;

    // Hands one acquired send chunk to the device and returns; PollSend() is
    // what waits for the completions that hand the chunks back.
    nbio::utility::expected<void, std::string> Send(std::span<char> chunk, std::size_t length) noexcept;
    nbio::async::Task<nbio::runtime, nbio::utility::expected<std::size_t, std::string>> PollSend(std::size_t count = 0);

    nbio::async::Task<nbio::runtime, nbio::utility::expected<std::optional<std::span<char>>, std::string>> Receive();

    // A message that has already arrived, without waiting for one: nothing when
    // none is ready. For a coroutine that has something else to do meanwhile.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<std::optional<std::span<char>>, std::string>> TryReceive();

    // Hands a received chunk back for the next message.
    nbio::utility::expected<void, std::string> Release(std::span<char> chunk) noexcept;

    nbio::net::RdmaConnector& connection() noexcept;
    const nbio::net::RdmaConnector& connection() const noexcept;

    RdmaSendChannel& send_channel() noexcept;
    const RdmaSendChannel& send_channel() const noexcept;

    RdmaReceiveChannel& receive_channel() noexcept;
    const RdmaReceiveChannel& receive_channel() const noexcept;

   private:
    nbio::net::RdmaConnector connection_;
    RdmaSendChannel send_channel_;
    RdmaReceiveChannel receive_channel_;
};
}  // namespace nbio::net

#endif  // defined(__linux__)
