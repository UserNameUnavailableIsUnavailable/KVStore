#pragma once

#include <cstdint>
#include <list>
#include <mutex>
#include <system_error>

namespace Foundation::Core
{
enum class SignalStatus
{
    kDone,
    kPending,
    kError
};

struct SignalResult
{
    SignalStatus status{SignalStatus::kPending};
    std::error_code error_code{};
};

class Signal
{
  public:
    Signal();
    ~Signal() noexcept;
    std::uintptr_t native_handle() const noexcept
    {
        return handle_;
    }
    SignalResult drain() const;

    void set_non_blocking(bool enabled = true);

  private:
    // IMPORTANT: one signal is consumed once
    // When a signal is received, broadcast it as an event to all holders.
    std::uintptr_t handle_;              // event handle
    static std::once_flag once_; // signal handlers can only be initialized once
    static std::mutex m_;
    static std::list<std::uintptr_t> handles_; // all registered handles
    std::list<std::uintptr_t>::iterator it_;   // iterator for the current handle in the list
};
} // namespace Foundation::Core
