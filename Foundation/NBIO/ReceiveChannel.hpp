#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <coroutine>


namespace Foundation::NBIO
{
struct ReceiveJob
{
    Foundation::Core::Buffer *buffer{nullptr};
    Foundation::Core::ReceiveResult result{};
};
// Simplex channel dedicated to receiving: one job, one waiter, interested only
// in the "readable" event.
class ReceiveChannel : public Foundation::NBIO::Channel
{
  public:
    explicit ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReceiveChannel() noexcept override;

    Foundation::NBIO::Task<Foundation::Core::ReceiveResult> receive(Foundation::Core::Buffer &buffer);

    void handle_event() override;

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    ReceiveJob &job() noexcept
    {
        return job_;
    }
    const ReceiveJob &job() const noexcept
    {
        return job_;
    }
    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }

  private:
    Foundation::Core::Socket &socket_;
    ReceiveJob job_;
    Foundation::Async::Coroutine waiter_;
};
} // namespace Foundation::NBIO
