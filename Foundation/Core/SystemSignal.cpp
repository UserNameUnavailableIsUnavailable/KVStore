#include "SystemSignal.hpp"

#include <csignal>
#include <mutex>
#include <stdexcept>
#include <system_error>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/eventfd.h>
#endif

#include <atomic>

namespace Foundation::Core
{

std::once_flag SystemSignal::once_;
std::mutex SystemSignal::m_;
std::list<std::uintptr_t> SystemSignal::handles_;
static std::atomic_size_t s_count{0};

SystemSignal::SystemSignal()
{
    {
        std::lock_guard<std::mutex> lock(m_);
        handle_ = static_cast<std::uintptr_t>(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
        handles_.push_back(handle_);
        it_ = std::prev(handles_.end());
    }
    if (handle_ == static_cast<std::uintptr_t>(-1))
    {
        throw std::runtime_error("failed to create eventfd");
    }
    std::call_once(once_, [] {
        // we can use std::signal in UNIX
        std::signal(SIGINT, [](int) {
            std::lock_guard<std::mutex> lock(m_);
            s_count++;
            if (handles_.empty())
            {
                SIG_DFL(SIGINT);
            }
            for (auto handle : handles_)
            {
                ::eventfd_write(handle, s_count);
            }
        });
        std::signal(SIGTERM, [](int) {
            std::lock_guard<std::mutex> lock(m_);
            if (handles_.empty())
            {
                SIG_DFL(SIGTERM);
            }
            for (auto handle : handles_)
            {
                ::eventfd_write(handle, s_count);
            }
        });
    });
}

void SystemSignal::non_blocking(bool enabled)
{
    int flags = ::fcntl(handle_, F_GETFL, 0);
    if (enabled)
    {
        flags |= O_NONBLOCK;
    }
    else
    {
        flags &= ~O_NONBLOCK;
    }
    if (::fcntl(handle_, F_SETFL, flags) < 0)
    {
        throw std::runtime_error("failed to set non-blocking mode");
    }
}

expected<void, std::error_code> SystemSignal::drain() const
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
            return unexpected<std::error_code>(std::make_error_code(std::errc::operation_would_block));
        }
        if (errno == EINTR)
        {
            continue;
        }
        return unexpected<std::error_code>(std::error_code(errno, std::system_category()));
    }
}

SystemSignal::~SystemSignal() noexcept
{
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    {
        std::lock_guard<std::mutex> lock(m_);
        handles_.erase(it_);
    }
    ::close(handle_);
}
} // namespace Foundation::Core
