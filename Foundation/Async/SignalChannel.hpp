#pragma once

#include <Foundation/Core/Signal.hpp>
#include <coroutine>
#include <list>

#include "Channel.hpp"

namespace Foundation::Async
{
// A channel over a shared Signal eventfd. A signal writes to every registered
// Signal instance; this channel then resumes every coroutine waiting on this
// engine's SignalService.
class SignalChannel final : public Channel
{
  public:
        SignalChannel(Foundation::Core::Signal &signal, Multiplexer &multiplexer, Scheduler &scheduler);
    ~SignalChannel() override;

    void on_event() override;

    void insert(std::coroutine_handle<> handle);
    bool remove(std::coroutine_handle<> handle);

    std::size_t &count() noexcept
    {
        return count_;
    }
    const std::size_t &count() const noexcept
    {
        return count_;
    }

    // Awaitable: suspends the caller until a signal is delivered.
    struct Awaiter
    {
        SignalChannel *channel;
        std::coroutine_handle<> handle_{};

        bool await_ready() const noexcept
        {
            return false;
        }
        void await_suspend(std::coroutine_handle<> handle) noexcept
        {
            handle_ = handle;
            channel->insert(handle);
        }
        void await_resume() noexcept
        {
            handle_ = {};
        }

        // cancellation detach: if this frame is destroyed while still parked,
        // drop the registration so a later signal never resumes a dead handle.
        ~Awaiter()
        {
            if (handle_)
            {
                channel->remove(handle_);
            }
        }
    };

    Awaiter wait() noexcept
    {
        return Awaiter{this};
    }

  private:
        Foundation::Core::Signal &signal_;
    std::list<std::coroutine_handle<>> waiters_;
    std::size_t count_;
};
} // namespace Foundation::Async
