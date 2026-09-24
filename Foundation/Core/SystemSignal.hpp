#pragma once

#include <cstdint>
#include <list>
#include <mutex>
#include <system_error>

#include "Expected.hpp"

namespace Foundation::Core
{
class SystemSignal
{
  public:
    SystemSignal();
    ~SystemSignal() noexcept;
    std::uintptr_t native_handle() const noexcept
    {
        return handle_;
    }
    expected<void, std::error_code> drain() const;

    void non_blocking(bool enabled = true);

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
