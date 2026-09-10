#include "Timer.hpp"

#include <fcntl.h>
#include <sys/timerfd.h>
#include <system_error>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <stdexcept>

namespace Foundation
{
Timer::Timer()
{
    handle_ = timerfd_create(CLOCK_MONOTONIC, 0);
    if (handle_ == -1)
    {
        throw std::runtime_error("failed to create timer");
    }
}

void Timer::set_non_blocking(bool enabled)
{
    int flags = fcntl(handle_, F_GETFL, 0);
    if (flags == -1)
    {
        throw std::runtime_error("failed to get file flags");
    }
    if (enabled)
    {
        flags |= O_NONBLOCK;
    }
    else
    {
        flags &= ~O_NONBLOCK;
    }
    if (fcntl(handle_, F_SETFL, flags) == -1)
    {
        throw std::runtime_error("failed to set file flags");
    }
}

Timer::~Timer() noexcept
{
    ::close(handle_);
}

void Timer::fire_at(std::chrono::steady_clock::time_point timepoint)
{
    auto now = std::chrono::steady_clock::now();
    auto duration = timepoint - now;
    if (duration.count() < 0)
    {
        duration = std::chrono::nanoseconds(1);
    }
    fire_after(duration);
}

void Timer::cancel()
#if defined(__linux__)
{
    itimerspec spec;
    std::memset(&spec, 0, sizeof(spec));
    timerfd_settime(handle_, 0, &spec, nullptr);
}

TimerResult Timer::wait()
{
    TimerResult result{.status = TimerStatus::kPending, .error_code = {}};
    bool retry{false};
    do
    {
        retry = false;
        auto n = ::read(handle_, &result.error_code, sizeof(result.error_code));
        if (n > 0)
        {
            result.status = TimerStatus::kDone;
        }
        else
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // pending
            }
            if (errno == EINTR)
            {
                retry = true;
            }
            else
            {
                result.status = TimerStatus::kError;
                result.error_code = std::error_code(errno, std::system_category());
            }
        }
    } while (retry);
    return result;
}
#endif
} // namespace Foundation
