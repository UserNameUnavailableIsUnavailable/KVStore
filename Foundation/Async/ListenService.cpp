#include "ListenService.hpp"

#include <sys/socket.h>

#include <Foundation/Socket.hpp>

#include "ListenChannel.hpp"

namespace Foundation::Async
{
ListenService::ListenService(const Foundation::Address &address, Multiplexer &multiplexer, Scheduler &scheduler, int backlog)
    : socket_(address.family(), Foundation::Socket::Type::kStream), channel_(socket_, multiplexer, scheduler)
{
    socket_.set_non_blocking();
    socket_.set_reuse_address(); // enable address reuse for quick restart
    socket_.bind(address);
    socket_.listen(backlog);
}

ListenService::~ListenService() noexcept
{
    // NOTE: channel automatically unregisters itself
}

Task<AcceptResult> ListenService::accept()
{
    auto result = co_await channel_.Accept();
    co_return std::move(result);
}
} // namespace Foundation::Async
