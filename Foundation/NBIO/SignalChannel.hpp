#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Signal.hpp>
#include <coroutine>
#include <cstddef>
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
    ~SignalChannel();

    void handle_completion();

    // The one-job protocol (see Channel.hpp), as the channels that carry a single
    // wait keep it. The backend is asked for a poll, not a read: the signals stay in
    // the signalfd until this channel drains them, so the wait itself is what is
    // prepared and a job has nothing to hold.
    bool submit_job();
    void advance_job(std::ptrdiff_t result) noexcept;
    void complete_job() noexcept;

    void park(Foundation::Async::Coroutine coroutine);

    const Core::Signal &signal() const noexcept
    {
        return signal_;
    }

    Core::Signal &signal() noexcept
    {
        return signal_;
    }

    SignalAwaiter wait() noexcept
    {
        return SignalAwaiter{*this};
    }

  private:
    Foundation::Core::Signal &signal_;
    std::list<Foundation::Async::Coroutine> waiters_;
    // Whether the poll that reports a signal is out there.
    bool submitted_{false};
};

template <typename PromiseType>
void SignalAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
{
    auto coroutine = Foundation::Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine));
}
} // namespace Foundation::NBIO
