#pragma once

#include <cstdint>
#include <system_error>

#include "Native.hpp"

namespace Foundation::Core
{
enum class NotifierStatus
{
    kDone,
    kPending,
    kError
};

struct NotifierResult
{
    NotifierStatus status;
    std::error_code error_code;
};

class Notifier
{
public:

    Notifier();
    ~Notifier() noexcept;

    Notifier(const Notifier &) = delete;
    Notifier &operator=(const Notifier &) = delete;
    Notifier(Notifier &&) = delete;
    Notifier &operator=(Notifier &&) = delete;

    std::uintptr_t native_handle() const noexcept { return handle_; }
    void set_non_blocking(bool enabled = true);
    void notify();
    NotifierResult wait();

private:
    std::uintptr_t handle_;
};
} // namespace Foundation::Core
