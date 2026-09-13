#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Socket.hpp>

namespace Foundation::NBIO
{
struct AcceptJob
{
    Foundation::Core::AcceptResult result;
};

// Simplex channel dedicated to accepting: one job, one waiter, interested only
// in the "readable" event of a listening socket.
class ListenChannel final : public Foundation::NBIO::Channel
{
  public:
    ListenChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexert, Foundation::Async::Scheduler &scheduler);
    ~ListenChannel() noexcept override;
    void handle_event() override;

    Foundation::NBIO::Task<Foundation::Core::AcceptResult> accept();

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    AcceptJob &job() noexcept
    {
        return job_;
    }
    const AcceptJob &job() const noexcept
    {
        return job_;
    }

    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }
    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }

  private:
    Foundation::Core::Socket socket_;
    AcceptJob job_;
    Foundation::Async::Coroutine waiter_;
};
} // namespace Foundation::NBIO
