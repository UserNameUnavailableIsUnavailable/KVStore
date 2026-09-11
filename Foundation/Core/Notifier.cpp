#include "Notifier.hpp"

#include <cstdint>
#include <fcntl.h>
#include <stdexcept>

#if defined(__linux__)
#include <sys/eventfd.h>
#include <unistd.h>
#endif

namespace Foundation::Core
{
Notifier::Notifier()
{
#if defined(__linux__)
    handle_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (handle_ < 0)
    {
        throw std::runtime_error("failed to create eventfd for Notifier");
    }
#else
    throw std::runtime_error("Notifier is only supported on Linux");
#endif
}

Notifier::~Notifier() noexcept
{
#if defined(__linux__)
    if (handle_ >= 0)
    {
        ::close(handle_);
    }
#endif
}

void Notifier::set_non_blocking(bool enabled)
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

void Notifier::notify()
{
#if defined(__linux__)
    constexpr uint64_t one = 1;
    if (::eventfd_write(handle_, one) != 0)
    {
        throw std::runtime_error("failed to notify eventfd");
    }
#else
    throw std::runtime_error("Notifier is only supported on Linux");
#endif
}

NotifierResult Notifier::wait()
{
    uint64_t value;
    NotifierResult result{.status = NotifierStatus::kPending, .error_code = {}};

    bool retry = false;
    do
    {
        retry = false;
        auto n = ::eventfd_read(handle_, &value);
        if (n > 0)
        {
            result.status = NotifierStatus::kDone;
        }
        else if (n == 0)
        {
            // result == 0 indicates no events, keep status as pending
        }
        else
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // no events available, keep status as pending
            }
            else if (errno == EINTR)
            {
                retry = true;
                continue;
            }
            else
            {
                result.status = NotifierStatus::kError;
                result.error_code = std::error_code(errno, std::system_category());
            }
        }
    } while (retry);
    return result;
}
} // namespace Foundation::Core
