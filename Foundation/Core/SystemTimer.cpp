#include "SystemTimer.hpp"
#include <cerrno>
#include <system_error>
#include <chrono>
#include <cstring>
#include <stdexcept>

#if defined(__linux__)
#elif defined(_WIN32)
#include <Windows.h>

namespace Foundation::Core
{
SystemTimer::SystemTimer()
{
	handle_ = ::CreateWaitableTimerW(nullptr, true, nullptr);
	if (!handle_)
	{
		std::error_code e(GetLastError(), std::system_category());

		throw std::runtime_error(
			std::format("::CreateWaitableTimerW() failed: {}", e.message())
		);
	}
}

SystemTimer::~SystemTimer() noexcept
{
	CloseHandle(handle_);
}
}
#endif

#if defined(_WIN32)
#endif

#if defined(__linux__)
#include <fcntl.h>
#include <sys/timerfd.h>
#include <unistd.h>

namespace Foundation::Core
{
SystemTimer::SystemTimer()
{
    auto handle = timerfd_create(CLOCK_MONOTONIC, 0);
    if (handle == -1)
    {
        throw std::runtime_error("failed to create timer");
    }
    handle_ = static_cast<std::uintptr_t>(handle);
}

void SystemTimer::non_blocking(bool enabled)
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

SystemTimer::~SystemTimer() noexcept
{
    ::close(handle_);
}

void SystemTimer::fire_at(std::chrono::steady_clock::time_point timepoint)
{
    auto now = std::chrono::steady_clock::now();
    auto duration = timepoint - now;
    if (duration.count() < 0)
    {
        duration = std::chrono::nanoseconds(1);
    }
    fire_after(duration);
}

void SystemTimer::cancel()
#if defined(__linux__)
{
    itimerspec spec;
    std::memset(&spec, 0, sizeof(spec));
    timerfd_settime(handle_, 0, &spec, nullptr);
}

expected<void, std::error_code> SystemTimer::wait()
{
    std::uint64_t expirations = 0;
    while (true)
    {
        const auto n = ::read(handle_, &expirations, sizeof(expirations));
        if (n > 0)
        {
            return {};
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return unexpected<std::error_code>(std::make_error_code(std::errc::operation_would_block));
        }
        if (errno == EINTR)
        {
            continue;
        }
        return unexpected<std::error_code>(std::error_code(errno, std::system_category()));
    }
}
#endif
} // namespace Foundation::Core
#endif
