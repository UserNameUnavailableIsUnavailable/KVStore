#pragma once

#include <cstdint>
#include <system_error>

#include "Expected.hpp"

namespace Foundation::Core
{
class EventNotifier
{
public:

    EventNotifier();
    ~EventNotifier() noexcept;

    EventNotifier(const EventNotifier &) = delete;
    EventNotifier &operator=(const EventNotifier &) = delete;
    EventNotifier(EventNotifier &&) = delete;
    EventNotifier &operator=(EventNotifier &&) = delete;

    std::uintptr_t native_handle() const noexcept { return handle_; }
    void non_blocking(bool enabled = true);
    void notify();
    expected<void, std::error_code> wait();

private:
    std::uintptr_t handle_;
};
} // namespace Foundation::Core
