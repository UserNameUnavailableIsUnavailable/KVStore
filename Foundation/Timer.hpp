#pragma once

#include <chrono>
#include <system_error>
#if defined(__linux__)
#include <sys/timerfd.h>
#endif

namespace Foundation
{
enum class TimerStatus
{
    kDone,
    kPending,
    kError
};

struct TimerResult
{
    TimerStatus status{TimerStatus::kPending};
    std::error_code error_code{};
};

class Timer
{
  public:
    using Clock = std::chrono::steady_clock;
    using Timepoint = Clock::time_point;
    using Duration = Clock::duration;
#if defined(__linux__)
    using Handle = int;
#endif
    Timer();
    ~Timer() noexcept;
    Timer(const Timer &) = delete;
    Timer &operator=(const Timer &) = delete;
    Timer(Timer &&) = delete;
    Timer &operator=(Timer &&) = delete;
    Handle get_native_handle()
    {
        return handle_;
    }
    void set_non_blocking(bool enabled = true);
    template <typename Rep, typename Period> void fire_after(std::chrono::duration<Rep, Period> duration);
    void fire_at(Timepoint timepoint);
    void cancel();
    TimerResult wait();

  private:
    Handle handle_;
};

template <typename Rep, typename Period> void Timer::fire_after(std::chrono::duration<Rep, Period> duration)
#if defined(__linux__)
{
    itimerspec spec;
    auto &sec = spec.it_value.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    auto &nsec = spec.it_value.tv_nsec =
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() % 1000000000;
    // if both sec and nsec are 0, the timer will be disarmed
    if (sec == 0 && nsec == 0)
    {
        nsec = 1;
    }
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 0;
    if (timerfd_settime(handle_, 0, &spec, nullptr) == -1)
    {
        throw std::runtime_error("failed to set timer");
    }
}
#endif
} // namespace Foundation
