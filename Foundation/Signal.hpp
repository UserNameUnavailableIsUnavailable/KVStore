#pragma once

#include <list>
#include <mutex>
#include <system_error>

namespace Foundation
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
    using Handle = int;
    Signal();
    ~Signal() noexcept;
    Handle get_native_handle() const noexcept
    {
        return handle_;
    }
    SignalResult observe() const;

    void set_non_blocking(bool enabled = true);

  private:
    // IMPORTANT: one signal is consumed once
    // When a signal is received, broadcast it as an event to all holders.
    Handle handle_;              // event handle
    static std::once_flag once_; // signal handlers can only be initialized once
    static std::mutex m_;
    static std::list<Handle> handles_; // all registered handles
    std::list<Handle>::iterator it_;   // iterator for the current handle in the list
};
} // namespace Foundation
