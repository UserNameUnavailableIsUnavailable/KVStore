#include "TcpSessionService.hpp"

#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <atomic>
#include <utility>

namespace Foundation::NBIO
{
TcpSessionService::TcpSessionService(Foundation::Core::TcpConnector connector) :
    connector_(std::move(connector)), receive_channel_(connector_, Engine::multiplexer(), Engine::scheduler()),
    send_channel_(connector_, Engine::multiplexer(), Engine::scheduler()),
    id_(next_id_.fetch_add(1, std::memory_order_acq_rel))
{
}

TcpSessionService::~TcpSessionService() noexcept = default;

void TcpSessionService::close() noexcept
{
    // The socket's own shutdown, then its close: a session is over once the descriptor
    // is gone, and the channels go on referring to a connection that is no longer
    // there -- which is what a reader still waiting on it needs to be told. Neither
    // reports anything worth acting on here.
    (void)connector_.shutdown();
    connector_.close();
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> TcpSessionService::receive(std::span<char> buffer)
{
    return receive_channel_.receive(buffer);
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> TcpSessionService::send(std::span<const char> buffer)
{
    return send_channel_.send(buffer);
}

std::atomic_uint TcpSessionService::next_id_{0};
} // namespace Foundation::NBIO
