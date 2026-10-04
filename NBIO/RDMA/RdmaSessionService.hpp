#pragma once
#if defined(__linux__)

#include <NBIO/RDMA/RdmaConnector.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <optional>
#include <span>
#include <string>

#include "RdmaReceiveChannel.hpp"
#include "RdmaSendChannel.hpp"

namespace NBIO::RDMA {
class Multiplexer;

class RdmaSessionService final {
   public:
    RdmaSessionService(NBIO::RDMA::RdmaConnector connection, NBIO::Core::Multiplexer& multiplexer,
                       NBIO::Async::Scheduler& scheduler);

    RdmaSessionService(const RdmaSessionService&) = delete;
    RdmaSessionService& operator=(const RdmaSessionService&) = delete;
    RdmaSessionService(RdmaSessionService&&) = delete;
    RdmaSessionService& operator=(RdmaSessionService&&) = delete;
    ~RdmaSessionService() noexcept = default;

    // Hands one acquired send chunk to the device and returns; poll_send() is
    // what waits for the completions that hand the chunks back.
    NBIO::Utility::expected<void, std::string> send(std::span<char> chunk, std::size_t length) noexcept;
    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::size_t, std::string>> poll_send(std::size_t count = 0);

    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::optional<std::span<char>>, std::string>> receive();

    // A message that has already arrived, without waiting for one: nothing when
    // none is ready. For a coroutine that has something else to do meanwhile.
    NBIO::Async::Task<NBIO::Runtime, NBIO::Utility::expected<std::optional<std::span<char>>, std::string>> try_receive();

    // Hands a received chunk back for the next message.
    NBIO::Utility::expected<void, std::string> release(std::span<char> chunk) noexcept;

    NBIO::RDMA::RdmaConnector& connection() noexcept;
    const NBIO::RDMA::RdmaConnector& connection() const noexcept;

    RdmaSendChannel& send_channel() noexcept;
    const RdmaSendChannel& send_channel() const noexcept;

    RdmaReceiveChannel& receive_channel() noexcept;
    const RdmaReceiveChannel& receive_channel() const noexcept;

   private:
    NBIO::RDMA::RdmaConnector connection_;
    RdmaSendChannel send_channel_;
    RdmaReceiveChannel receive_channel_;
};
}  // namespace NBIO::RDMA

#endif  // defined(__linux__)
