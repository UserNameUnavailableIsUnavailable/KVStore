#include "Session.hpp"
#include <Foundation/NBIO/Runtime.hpp>

#include <atomic>

namespace Foundation::NBIO
{
Session::Session(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : socket_(std::move(socket)), multiplexer_(multiplexer), receive_channel_(socket_, multiplexer, scheduler),
      send_channel_(socket_, scheduler, multiplexer), id_(next_id_.fetch_add(1, std::memory_order_acq_rel))
{
}

Session::~Session() noexcept = default;

void Session::close() noexcept
{
    socket_.shutdown();
    socket_.close();
}

Foundation::NBIO::Task<std::optional<std::size_t>> Session::receive(std::span<char> buffer)
{
    return receive_channel_.receive(buffer);
}

Foundation::NBIO::Task<std::optional<std::size_t>> Session::send(std::span<const char> buffer)
{
    return send_channel_.send(buffer);
}

std::atomic_uint Session::next_id_{0};
} // namespace Foundation::NBIO
