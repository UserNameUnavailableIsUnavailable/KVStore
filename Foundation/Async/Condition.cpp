#include "Condition.hpp"
#include <coroutine>
#include <mutex>

namespace Foundation::Async::detail
{
ConditionAwaiter::ConditionAwaiter(Condition& condition) :
    condition(condition)
{
}

ConditionAwaiter::~ConditionAwaiter() noexcept
{
    condition.remove(handle);
}

bool ConditionAwaiter::await_ready() noexcept
{
    return false;
}

void ConditionAwaiter::await_suspend(std::coroutine_handle<> h)
{
    handle = h;
    condition.insert(h);
}

void ConditionAwaiter::await_resume() noexcept
{
}

void Condition::insert(std::coroutine_handle<> h)
{
    std::lock_guard lock(mutex_);
    notifiees_.emplace_back(h);
    if (!channel_.armed())
    {
        channel_.arm();
    }
}

ConditionAwaiter Condition::wait()
{
    return ConditionAwaiter(*this);
}

Condition::Condition(NotifyChannel& channel) :
    channel_(channel)
{
}

Condition::~Condition() noexcept
{
    std::lock_guard lock(mutex_);
    assert(notifiees_.empty() && "destroying Condition while waiters still exist");
}

bool Condition::remove(std::coroutine_handle<> h)
{
    if (!h)
    {
        return true;
    }
    std::lock_guard lock(mutex_);
    for (auto& waiter : notifiees_)
    {
        if (waiter == h)
        {
            waiter = {}; // lazy erasure
        }
    }
    return true;
}

void Condition::notify_one()
{
    std::coroutine_handle<> waiter;
    {
        std::lock_guard lock(mutex_);
        while (!notifiees_.empty())
        {
            waiter = notifiees_.front();
            if (waiter)
            {
                notifiees_.pop_front();
                break;
            }
        }
    }
    if (waiter)
    {
        channel_.submit(waiter);
    }
}

void Condition::notify_all()
{
    std::deque<std::coroutine_handle<>> notifiees;
    {
        std::lock_guard lock(mutex_);
        notifiees = notifiees_;
        notifiees_.clear();
    }

    channel_.submit(notifiees.begin(), notifiees.end());
}
} // namespace Foundation::Async::detail