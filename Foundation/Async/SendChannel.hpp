#pragma once

#include <Foundation/Buffer.hpp>
#include <Foundation/Socket.hpp>
#include <coroutine>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
struct SendJob
{
    Buffer *buffer{nullptr};
    SendResult result{};
};
// Simplex channel dedicated to sending: one job, one waiter, interested only
// in the "writable" event.
class SendChannel final : public Channel
{
  public:
    explicit SendChannel(Foundation::Socket &socket, Scheduler &scheduler, Multiplexer &multiplexer);
    ~SendChannel() noexcept override;

    Task<SendResult> send(::Foundation::Buffer &buffer);

    void on_event() override;

    void set_waiter(std::coroutine_handle<> co) noexcept
    {
        waiter_ = co;
    }
    std::coroutine_handle<> get_waiter() const noexcept
    {
        return waiter_;
    }

    SendJob &job() noexcept
    {
        return job_;
    }
    const SendJob &job() const noexcept
    {
        return job_;
    }
    Foundation::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Socket &socket() const noexcept
    {
        return socket_;
    }

  private:
    Foundation::Socket &socket_;
    SendJob job_;
    std::coroutine_handle<> waiter_;
};
} // namespace Foundation::Async
