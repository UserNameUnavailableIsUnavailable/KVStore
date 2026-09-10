#pragma once

#include <Foundation/Socket.hpp>

#include "ListenChannel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"

namespace Foundation::Async
{
class ListenService
{
  public:
    ListenService(const Foundation::Address &address, Multiplexer &mux, Scheduler &scheduler, int backlog = 4096);
    ~ListenService() noexcept;
    ListenService(const ListenService &) = delete;
    ListenService &operator=(const ListenService &) = delete;
    ListenService(ListenService &&) = delete;
    ListenService &operator=(ListenService &&) = delete;

    Task<Foundation::AcceptResult> accept();

    Foundation::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Socket &socket() const noexcept
    {
        return socket_;
    }

    Channel &channel() noexcept
    {
        return channel_;
    }
    const Channel &channel() const noexcept
    {
        return channel_;
    }

  private:
    // NOTE:
    // channel keeps a pointer to socket
    // we MUST use unique_ptr here to avoid dangling pointer
    Foundation::Socket socket_;
    ListenChannel channel_;
};
} // namespace Foundation::Async
