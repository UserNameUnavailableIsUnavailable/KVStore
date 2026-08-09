#pragma once

#if not defined(__linux__)
#error "This header is linux-specific."
#endif

#include <chrono>
#include <cstdint>
#include <format>
#include <stdexcept>
#include <utility>

#include <sys/timerfd.h>
#include <unistd.h>

namespace KV
{
// Timer owns a Linux timerfd.
//
// The fd becomes readable when the timer expires; reading it yields the number
// of expirations that have accumulated since the last read.  That makes a timer
// just another pollable handle, so epoll and io_uring can wait on it exactly
// the way they wait on a socket -- no separate timing mechanism is needed.
//
// The clock is CLOCK_MONOTONIC: wall-clock adjustments (NTP, manual date
// changes) must not stretch or shrink an interval that was already scheduled.
class Timer
{
public:
    using HandleType = int;
    // Every public duration is normalised to nanoseconds at the boundary so
    // that timers of different duration types remain a single, storable type.
    using Duration = std::chrono::nanoseconds;

    Timer()
    {
        handle_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (handle_ < 0)
        {
            throw std::runtime_error(std::format("failed to create timerfd, errno: {}", errno));
        }
    }

    // Create and arm in one step, so a timer is never observable in an
    // armed-but-not-yet-scheduled state.
    template <typename Rep, typename Period>
    explicit Timer(std::chrono::duration<Rep, Period> timeout) : Timer()
    {
        SetTimeout(timeout);
    }

    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;

    Timer(Timer&& other) noexcept : handle_(std::exchange(other.handle_, kInvalidHandle)) {}

    Timer& operator=(Timer&& other) noexcept
    {
        if (this != &other)
        {
            Close();
            handle_ = std::exchange(other.handle_, kInvalidHandle);
        }
        return *this;
    }

    ~Timer() noexcept
    {
        Close();
    }

    HandleType GetNativeHandle() const noexcept
    {
        return handle_;
    }

    bool IsOpen() const noexcept
    {
        return handle_ != kInvalidHandle;
    }

    // Fire once, *timeout* from now.
    template <typename Rep, typename Period>
    void SetTimeout(std::chrono::duration<Rep, Period> timeout)
    {
        Arm(std::chrono::duration_cast<Duration>(timeout), Duration::zero());
    }

    // Fire once at an absolute point on the steady clock.
    //
    // Absolute deadlines are what a timer heap wants: re-arming for the new
    // earliest deadline never re-derives a delay, so a long-lived periodic task
    // cannot accumulate drift from the scheduling latency of each tick.
    //
    // This relies on std::chrono::steady_clock being CLOCK_MONOTONIC, which is
    // how libstdc++ and libc++ implement it on Linux -- the same clock the fd
    // was created with.
    void SetDeadline(std::chrono::steady_clock::time_point deadline)
    {
        const ::itimerspec specification {
            .it_interval = {.tv_sec = 0, .tv_nsec = 0},
            .it_value = ToTimespec(deadline.time_since_epoch()),
        };
        if (::timerfd_settime(handle_, TFD_TIMER_ABSTIME, &specification, nullptr) < 0)
        {
            throw std::runtime_error(std::format("failed to arm timerfd, errno: {}", errno));
        }
    }

    // Fire every *interval*, starting one interval from now.
    template <typename Rep, typename Period>
    void SetInterval(std::chrono::duration<Rep, Period> interval)
    {
        const Duration period = std::chrono::duration_cast<Duration>(interval);
        Arm(period, period);
    }

    void Disarm()
    {
        const ::itimerspec specification {};
        if (::timerfd_settime(handle_, 0, &specification, nullptr) < 0)
        {
            throw std::runtime_error(std::format("failed to disarm timerfd, errno: {}", errno));
        }
    }

    bool IsArmed() const
    {
        ::itimerspec specification {};
        if (::timerfd_gettime(handle_, &specification) < 0)
        {
            throw std::runtime_error(std::format("failed to query timerfd, errno: {}", errno));
        }
        return specification.it_value.tv_sec != 0 || specification.it_value.tv_nsec != 0;
    }

    // Consume the pending expiration count.  A timerfd stays readable until it
    // is read, so a level-triggered poller would spin forever if this were
    // skipped.  Returns 0 when the timer has not expired yet.
    std::uint64_t Drain() noexcept
    {
        std::uint64_t expirations = 0;
        const ::ssize_t received = ::read(handle_, &expirations, sizeof(expirations));
        return received == static_cast<::ssize_t>(sizeof(expirations)) ? expirations : 0;
    }

private:
    static constexpr HandleType kInvalidHandle = -1;

    void Arm(Duration initial, Duration period)
    {
        // An all-zero it_value means "disarm" to timerfd_settime, so a zero or
        // negative delay would silently never fire and whoever waits on this
        // timer would hang forever.  Round it up to the smallest real delay
        // instead: "as soon as possible" is what the caller meant.
        if (initial <= Duration::zero())
        {
            initial = Duration(1);
        }
        const ::itimerspec specification {
            .it_interval = ToTimespec(period),
            .it_value = ToTimespec(initial),
        };
        if (::timerfd_settime(handle_, 0, &specification, nullptr) < 0)
        {
            throw std::runtime_error(std::format("failed to arm timerfd, errno: {}", errno));
        }
    }

    static ::timespec ToTimespec(Duration duration) noexcept
    {
        if (duration <= Duration::zero())
        {
            return {.tv_sec = 0, .tv_nsec = 0};
        }
        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
        return {.tv_sec = static_cast<::time_t>(seconds.count()),
            .tv_nsec = static_cast<long>((duration - seconds).count())};
    }

    void Close() noexcept
    {
        if (handle_ != kInvalidHandle)
        {
            ::close(handle_);
            handle_ = kInvalidHandle;
        }
    }

    HandleType handle_ = kInvalidHandle;
};
} // namespace KV
