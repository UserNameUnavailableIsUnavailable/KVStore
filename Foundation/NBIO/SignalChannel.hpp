#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Signal.hpp>
#include <coroutine>
#include <list>


namespace Foundation::NBIO
{
class SignalChannel;

struct SignalAwaiter
{
    SignalChannel &channel;

    bool await_ready() const noexcept
    {
        return false;
    }

    template <typename PromiseType> void
    await_suspend(std::coroutine_handle<PromiseType> handle) noexcept;

    void await_resume() noexcept
    {
    }

    // cancellation detach: if this frame is destroyed while still parked,
    // drop the registration so a later signal never resumes a dead handle.
    ~SignalAwaiter() noexcept = default;
};

// A channel over a shared Signal eventfd. A signal writes to every registered
// Signal instance; this channel then resumes every coroutine waiting on this
// engine's signal channel.
class SignalChannel final : public Foundation::NBIO::Channel
{
  public:
    SignalChannel(Foundation::Core::Signal &signal, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~SignalChannel() override;

    void handle_event() override;

    void park(Foundation::Async::Coroutine coroutine);

    const Core::Signal &signal() const noexcept
    {
        return signal_;
    }

    Core::Signal &signal() noexcept
    {
        return signal_;
    }

    std::size_t &count() noexcept
    {
        return count_;
    }

    const std::size_t &count() const noexcept
    {
        return count_;
    }


    SignalAwaiter wait() noexcept
    {
        return SignalAwaiter{*this};
    }

  private:
    Foundation::Core::Signal &signal_;
    std::list<Foundation::Async::Coroutine> waiters_;
    std::size_t count_;
};

template <typename PromiseType>
void SignalAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
{
    auto coroutine = Foundation::Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine));
}
} // namespace Foundation::NBIO
