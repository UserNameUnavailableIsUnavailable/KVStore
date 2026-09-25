#pragma once
#if defined(__linux__)

#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "RdmaReceiveChannel.hpp"
#include "RdmaSendChannel.hpp"

#include <optional>
#include <span>
#include <string>

namespace Foundation::NBIO
{
class Multiplexer;

class RdmaSessionService final
{
  public:
    RdmaSessionService(Foundation::Core::RdmaConnector connection, Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);

    RdmaSessionService(const RdmaSessionService &) = delete;
    RdmaSessionService &operator=(const RdmaSessionService &) = delete;
    RdmaSessionService(RdmaSessionService &&) = delete;
    RdmaSessionService &operator=(RdmaSessionService &&) = delete;
    ~RdmaSessionService() noexcept = default;

    // Hands one acquired send chunk to the device and returns; poll_send() is
    // what waits for the completions that hand the chunks back.
    Core::expected<void, std::string> send(std::span<char> chunk, std::size_t length) noexcept;
    Task<Core::expected<std::size_t, std::string>> poll_send(std::size_t count = 0);

    Task<Core::expected<std::optional<std::span<char>>, std::string>> receive();

    // A message that has already arrived, without waiting for one: nothing when
    // none is ready. For a coroutine that has something else to do meanwhile.
    Task<Core::expected<std::optional<std::span<char>>, std::string>> try_receive();

    // Hands a received chunk back for the next message.
    Core::expected<void, std::string> release(std::span<char> chunk) noexcept;

    Foundation::Core::RdmaConnector &connection() noexcept;
    const Foundation::Core::RdmaConnector &connection() const noexcept;

    RdmaSendChannel &send_channel() noexcept;
    const RdmaSendChannel &send_channel() const noexcept;

    RdmaReceiveChannel &receive_channel() noexcept;
    const RdmaReceiveChannel &receive_channel() const noexcept;

  private:
    Foundation::Core::RdmaConnector connection_;
    RdmaSendChannel send_channel_;
    RdmaReceiveChannel receive_channel_;
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)