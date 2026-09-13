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
struct SendJob
{
    Foundation::Core::Buffer *buffer{nullptr};
    Foundation::Core::SendResult result{};
};
// Simplex channel dedicated to sending: one job, one waiter, interested only
// in the "writable" event.
class SendChannel final : public Foundation::NBIO::Channel
{
  public:
    explicit SendChannel(Foundation::Core::Socket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer);
    ~SendChannel() noexcept override;

    Foundation::NBIO::Task<Foundation::Core::SendResult> send(Foundation::Core::Buffer &buffer);

    void handle_event() override;

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    SendJob &job() noexcept
    {
        return job_;
    }
    const SendJob &job() const noexcept
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
    SendJob job_;
    Foundation::Async::Coroutine waiter_;
};
} // namespace Foundation::NBIO
