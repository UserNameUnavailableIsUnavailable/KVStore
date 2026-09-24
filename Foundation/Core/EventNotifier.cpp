#include "EventNotifier.hpp"

#include <cstdint>
#include <fcntl.h>
#include <stdexcept>

#if defined(__linux__)
#include <sys/eventfd.h>
#include <unistd.h>
#endif

namespace Foundation::Core
{
EventNotifier::EventNotifier()
{
#if defined(__linux__)
    handle_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (handle_ < 0)
    {
        throw std::runtime_error("failed to create eventfd for EventNotifier");
    }
#else
    throw std::runtime_error("EventNotifier is only supported on Linux");
#endif
}

EventNotifier::~EventNotifier() noexcept
{
#if defined(__linux__)
    if (handle_ >= 0)
    {
        ::close(handle_);
    }
#endif
}

void EventNotifier::non_blocking(bool enabled)
{
#if defined(__linux__)
    const int flags = ::fcntl(handle_, F_GETFL, 0);
    if (flags < 0)
    {
        throw std::runtime_error("failed to read notifier flags");
    }

    int updated_flags = flags;
    if (enabled)
    {
        updated_flags |= O_NONBLOCK;
    }
    else
    {
        updated_flags &= ~O_NONBLOCK;
    }

    if (::fcntl(handle_, F_SETFL, updated_flags) < 0)
    {
        throw std::runtime_error("failed to set non-blocking mode on notifier");
    }
#else
    (void)enabled;
#endif
}

void EventNotifier::notify()
{
#if defined(__linux__)
    constexpr uint64_t one = 1;
    if (::eventfd_write(handle_, one) != 0)
    {
        throw std::runtime_error("failed to notify eventfd");
    }
#else
    throw std::runtime_error("EventNotifier is only supported on Linux");
#endif
}

expected<void, std::error_code> EventNotifier::wait()
{
    uint64_t value = 0;
    while (true)
    {
        const auto n = ::eventfd_read(handle_, &value);
        if (n > 0)
        {
            return {};
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return unexpected(std::make_error_code(std::errc::operation_would_block));
        }
        if (errno == EINTR)
        {
            continue;
        }
        return unexpected<std::error_code>(std::error_code(errno, std::system_category()));
    }
}
} // namespace Foundation::Core
