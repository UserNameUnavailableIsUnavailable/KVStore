#include "RdmaSession.hpp"

namespace Foundation::NBIO
{
RdmaSession::RdmaSession(Foundation::Core::RdmaConnector connection, Multiplexer &multiplexer,
                           Foundation::Async::Scheduler &scheduler) :
    connection_(std::move(connection)), send_channel_(connection_, multiplexer, scheduler), receive_channel_(connection_, multiplexer, scheduler)
{
}

Core::expected<void, std::string> RdmaSession::send(std::span<char> chunk, std::size_t length) noexcept
{
    return send_channel_.send(chunk, length);
}

Task<Core::expected<std::size_t, std::string>> RdmaSession::poll_send(std::size_t count)
{
    co_return co_await send_channel_.poll(count);
}

Task<Core::expected<std::optional<std::span<char>>, std::string>> RdmaSession::receive()
{
    co_return co_await receive_channel_.receive();
}

Task<Core::expected<std::optional<std::span<char>>, std::string>> RdmaSession::try_receive()
{
    co_return co_await receive_channel_.try_receive();
}

Core::expected<void, std::string> RdmaSession::release(std::span<char> chunk) noexcept
{
    return receive_channel_.release(chunk);
}

Foundation::Core::RdmaConnector &RdmaSession::connection() noexcept
{
    return connection_;
}

const Foundation::Core::RdmaConnector &RdmaSession::connection() const noexcept
{
    return connection_;
}

RdmaSendChannel &RdmaSession::send_channel() noexcept
{
    return send_channel_;
}

const RdmaSendChannel &RdmaSession::send_channel() const noexcept
{
    return send_channel_;
}

RdmaReceiveChannel &RdmaSession::receive_channel() noexcept
{
    return receive_channel_;
}

const RdmaReceiveChannel &RdmaSession::receive_channel() const noexcept
{
    return receive_channel_;
}
} // namespace Foundation::NBIO