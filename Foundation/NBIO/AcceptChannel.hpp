#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/Address.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Socket.hpp>
#include <system_error>

namespace Foundation::NBIO
{
struct AcceptJob
{
    Foundation::Core::AcceptResult result;
};

class AcceptChannel final : public Foundation::NBIO::Channel
{
  public:
    AcceptChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~AcceptChannel() noexcept override;
    void handle_event() override;

    Foundation::NBIO::Task<std::optional<std::pair<Core::Socket, Core::Address>>> accept();

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

    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    Foundation::Core::Socket socket_;
    AcceptJob job_;
    Foundation::Async::Coroutine waiter_;
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
