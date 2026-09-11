#pragma once

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <coroutine>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
struct SendJob
{
    Foundation::Core::Buffer *buffer{nullptr};
    Foundation::Core::SendResult result{};
};
// Simplex channel dedicated to sending: one job, one waiter, interested only
// in the "writable" event.
class SendChannel final : public Channel
{
  public:
    explicit SendChannel(Foundation::Core::Socket &socket, Scheduler &scheduler, Multiplexer &multiplexer);
    ~SendChannel() noexcept override;

    Task<Foundation::Core::SendResult> send(Foundation::Core::Buffer &buffer);

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
    std::coroutine_handle<> waiter_;
};
} // namespace Foundation::Async
