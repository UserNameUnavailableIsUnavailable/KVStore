#pragma once

#include <Foundation/Socket.hpp>
#include <coroutine>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
struct AcceptJob
{
    Foundation::AcceptResult result;
};

// Simplex channel dedicated to accepting: one job, one waiter, interested only
// in the "readable" event of a listening socket.
class ListenChannel final : public Channel
{
  public:
    ListenChannel(Foundation::Socket &socket, Multiplexer &multiplexert, Scheduler &scheduler);
    ~ListenChannel() noexcept override;
    void on_event() override;

    Task<Foundation::AcceptResult> Accept();

    void Setwaiter(std::coroutine_handle<> co) noexcept
    {
        waiter_ = co;
    }
    std::coroutine_handle<> Getwaiter() const noexcept
    {
        return waiter_;
    }

    AcceptJob &job() noexcept
    {
        return job_;
    }
    const AcceptJob &job() const noexcept
    {
        return job_;
    }

    const Foundation::Socket &socket() const noexcept
    {
        return socket_;
    }
    Foundation::Socket &socket() noexcept
    {
        return socket_;
    }

  private:
    Foundation::Socket &socket_;
    AcceptJob job_;
    std::coroutine_handle<> waiter_;
};
} // namespace Foundation::Async
