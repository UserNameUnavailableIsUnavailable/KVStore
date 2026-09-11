#pragma once

#include <system_error>


namespace Foundation
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
    using Handle = int;
    Notifier();
    ~Notifier() noexcept;

    Notifier(const Notifier &) = delete;
    Notifier &operator=(const Notifier &) = delete;
    Notifier(Notifier &&) = delete;
    Notifier &operator=(Notifier &&) = delete;

    Handle native_handle() const noexcept { return handle_; }
    void set_non_blocking(bool enabled = true);
    void notify();
    NotifierResult wait();

private:
    Handle handle_{-1};
};
} // namespace Foundation
