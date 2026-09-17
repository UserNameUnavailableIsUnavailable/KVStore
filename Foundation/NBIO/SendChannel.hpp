#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <span>
#include <system_error>

namespace Foundation::NBIO
{
struct SendJob
{
    std::span<const char> buffer;
    Foundation::Core::SendResult result{};
};
// Simplex channel dedicated to sending: one job, one waiter, interested only
// in the "writable" event.
class SendChannel final : public Foundation::NBIO::Channel
{
  public:
    explicit SendChannel(Foundation::Core::Socket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer);
    ~SendChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> send(std::span<const char> buffer);

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
    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    Foundation::Core::Socket &socket_;
    SendJob job_;
    Foundation::Async::Coroutine waiter_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
