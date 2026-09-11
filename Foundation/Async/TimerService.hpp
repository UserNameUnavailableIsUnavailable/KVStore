#pragma once

#include <Foundation/Core/Timer.hpp>
#include <chrono>

#include "TimerChannel.hpp"

namespace Foundation::Async
{
class TimerService
{
  public:
    TimerService(Multiplexer &multiplexer, Scheduler &scheduler);

    TimerService(const TimerService &) = delete;
    TimerService &operator=(const TimerService &) = delete;
    TimerService(TimerService &&) = delete;
    TimerService &operator=(TimerService &&) = delete;

    detail::SleepAwaiter SleepUntil(std::chrono::steady_clock::time_point timepoint) noexcept
    {
        return channel_.sleep(timepoint);
    }

    template <typename Rep, typename Period> detail::SleepAwaiter SleepFor(std::chrono::duration<Rep, Period> duration)
    {
        return SleepUntil(Foundation::Core::Timer::Clock::now() + duration);
    }

    TimerChannel &channel() noexcept
    {
        return channel_;
    }
    const TimerChannel &channel() const noexcept
    {
        return channel_;
    }

  private:
    Foundation::Core::Timer timer_;
    TimerChannel channel_;
};
} // namespace Foundation::Async
