#include "TcpSession.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <atomic>

namespace Foundation::NBIO
{
TcpSession::TcpSession(Foundation::Core::TcpSocket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : socket_(std::move(socket)), multiplexer_(multiplexer), receive_channel_(socket_, multiplexer, scheduler),
      send_channel_(socket_, scheduler, multiplexer), id_(next_id_.fetch_add(1, std::memory_order_acq_rel))
{
}

TcpSession::~TcpSession() noexcept = default;

void TcpSession::close() noexcept
{
    socket_.shutdown();
    socket_.close();
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> TcpSession::receive(std::span<char> buffer)
{
    return receive_channel_.receive(buffer);
}

Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> TcpSession::send(std::span<const char> buffer)
{
    return send_channel_.send(buffer);
}

std::atomic_uint TcpSession::next_id_{0};
} // namespace Foundation::NBIO
