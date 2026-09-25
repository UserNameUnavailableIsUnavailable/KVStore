#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <chrono>

namespace Foundation::NBIO
{
// The thread's timer, as a handle: `co_await SystemTimeService{}.sleep(1s)`.
//
// The timer itself belongs to the runtime -- one timerfd per thread, which is what
// makes a sleep cheap and what lets any number of them be in flight at once -- so a
// handle owns nothing and can be made anywhere.
//
// That is also why it resolves the runtime where the wait is queued rather than where
// the handle was made. What a sleep belongs to is the engine driving the coroutine
// that awaits it, so a handle handed to a task running on another thread is still the
// right handle; one that captured a reference in its constructor would be pinned to
// the thread that made it and would outlive nothing. Resolving late also means the
// handle is stateless, so a temporary is as good as a named one -- and the failure
// case is the honest one: a sleep on a thread with no runtime throws where it is
// awaited, naming the thing that is actually missing.
class SystemTimeService final
{
  public:
    SystemTimeService() noexcept = default;

    // Suspends until `duration` has passed since this call, or until `point`: the
    // same timer either way.
    Foundation::NBIO::Task<void> sleep(std::chrono::steady_clock::duration duration) const
    {
        // Read now rather than when the task first runs, so a sleep of a duration means
        // the duration the caller asked for.
        return SleepUntil(std::chrono::steady_clock::now() + duration);
    }

    Foundation::NBIO::Task<void> sleep(std::chrono::steady_clock::time_point point) const
    {
        return SleepUntil(point);
    }

  private:
    // The sleep body, as a plain coroutine, and inline because it lives in a header.
    static Foundation::NBIO::Task<void> SleepUntil(std::chrono::steady_clock::time_point point)
    {
        co_await Engine::timer_channel().sleep(point);
    }
};
} // namespace Foundation::NBIO
