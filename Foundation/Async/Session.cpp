#include "Session.hpp"

#include <atomic>

namespace Foundation::Async
{
Session::Session(Foundation::Core::Socket socket, Multiplexer &multiplexer, Scheduler &scheduler)
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

Task<Foundation::Core::ReceiveResult> Session::receive(Foundation::Core::Buffer &buffer)
{
    return receive_channel_.receive(buffer);
}

Task<Foundation::Core::SendResult> Session::send(Foundation::Core::Buffer &buffer)
{
    return send_channel_.send(buffer);
}

std::atomic_uint Session::next_id_{0};
} // namespace Foundation::Async
