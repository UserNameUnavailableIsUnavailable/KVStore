#pragma once

#include "Expected.hpp"
#include <chrono>
#include <system_error>
#if defined(__linux__)
#include <sys/timerfd.h>
#endif

namespace Foundation::Core
{
class SystemTimer
{
  public:
#if defined(__unix__)
    using Handle = int;
#elif defined(_WIN32)
    using Handle = void*;
#endif
    
    using Clock = std::chrono::steady_clock;
    using Timepoint = Clock::time_point;
    using Duration = Clock::duration;
    
    SystemTimer();
    ~SystemTimer() noexcept;
    SystemTimer(const SystemTimer &) = delete;
    SystemTimer &operator=(const SystemTimer &) = delete;
    SystemTimer(SystemTimer &&) = delete;
    SystemTimer &operator=(SystemTimer &&) = delete;
    Handle native_handle()
    {
        return handle_;
    }
    void non_blocking(bool enabled = true);
    template <typename Rep, typename Period> void fire_after(std::chrono::duration<Rep, Period> duration);
    void fire_at(Timepoint timepoint);
    void cancel();
    expected<void, std::error_code> wait();

  private:
    Handle handle_;
};

template <typename Rep, typename Period> void SystemTimer::fire_after(std::chrono::duration<Rep, Period> duration)
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
} // namespace Foundation::Core
