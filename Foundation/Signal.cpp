#include "Signal.hpp"

#include <csignal>
#include <mutex>
#include <stdexcept>
#include <system_error>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/eventfd.h>
#endif

#include <atomic>

namespace Foundation
{

std::once_flag Signal::once_;
std::mutex Signal::m_;
std::list<Signal::Handle> Signal::handles_;
static std::atomic_size_t s_count{0};

Signal::Signal()
{
    {
        std::lock_guard<std::mutex> lock(m_);
        handle_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        handles_.push_back(handle_);
        it_ = std::prev(handles_.end());
    }
    if (handle_ < 0)
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

void Signal::set_non_blocking(bool enabled)
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

SignalResult Signal::observe() const
{
    uint64_t value;
    SignalResult result{.status = SignalStatus::kPending, .error_code = {}};

    bool retry = false;
    do
    {
        retry = false;
        auto n = ::eventfd_read(handle_, &value);
        if (n > 0)
        {
            result.status = SignalStatus::kDone;
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
                result.status = SignalStatus::kError;
                result.error_code = std::error_code(errno, std::system_category());
            }
        }
    } while (retry);
    return result;
}

Signal::~Signal() noexcept
{
    std::signal(SIGINT, SIG_DFL);
    std::signal(SIGTERM, SIG_DFL);
    {
        std::lock_guard<std::mutex> lock(m_);
        handles_.erase(it_);
    }
    ::close(handle_);
}
} // namespace Foundation
