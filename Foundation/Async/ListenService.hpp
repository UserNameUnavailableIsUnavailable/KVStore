#pragma once

#include <Foundation/Core/Socket.hpp>

#include "ListenChannel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"

namespace Foundation::Async
{
class ListenService
{
  public:
    ListenService(const Foundation::Core::Address &address, Multiplexer &mux, Scheduler &scheduler, int backlog = 4096);
    ~ListenService() noexcept;
    ListenService(const ListenService &) = delete;
    ListenService &operator=(const ListenService &) = delete;
    ListenService(ListenService &&) = delete;
    ListenService &operator=(ListenService &&) = delete;

    Task<Foundation::Core::AcceptResult> accept();

    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::Socket &socket() const noexcept
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
    Foundation::Core::Socket socket_;
    ListenChannel channel_;
};
} // namespace Foundation::Async
