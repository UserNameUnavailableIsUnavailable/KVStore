#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/SystemSignal.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <coroutine>
#include <cstddef>
#include <list>


namespace Foundation::NBIO
{
class SystemSignalChannel;

struct SystemSignalAwaiter
{
    SystemSignalChannel &channel;

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
    ~SystemSignalAwaiter() noexcept = default;
};

// A channel over a shared SystemSignal eventfd. A signal writes to every registered
// SystemSignal instance; this channel then resumes every coroutine waiting on this
// engine's signal channel.
class SystemSignalChannel final : public Foundation::NBIO::Channel
{
  public:
    SystemSignalChannel(Foundation::Core::SystemSignal &signal, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~SystemSignalChannel();

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload &submit();
    void complete();

    void park(Foundation::Async::Coroutine coroutine);

    const Core::SystemSignal &signal() const noexcept
    {
        return signal_;
    }

    Core::SystemSignal &signal() noexcept
    {
        return signal_;
    }

    SystemSignalAwaiter wait() noexcept
    {
        return SystemSignalAwaiter{*this};
    }

  private:
    Foundation::Core::SystemSignal &signal_;
    std::list<Foundation::Async::Coroutine> waiters_;
    Payload payload_{SystemSignalPayload{}};
};

template <typename PromiseType>
void SystemSignalAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) noexcept
{
    auto coroutine = Foundation::Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine));
}
} // namespace Foundation::NBIO
