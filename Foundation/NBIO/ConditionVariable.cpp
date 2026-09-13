#include "ConditionVariable.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include "Engine.hpp"

#include <algorithm>
#include <atomic>
#include <utility>

namespace Foundation::NBIO
{
ConditionVariable::ConditionVariable() :
    channel_(Engine::notify_channel())
{
}

ConditionVariable::~ConditionVariable() noexcept
{
    std::lock_guard lock(mutex_);
    assert(notifiees_.empty() && "destroying ConditionVariable while waiters still exist");
}

void ConditionVariable::notify_one()
{
    std::lock_guard lock(mutex_);
    while (!notifiees_.empty())
    {
        Async::Coroutine waiter = std::move(notifiees_.front());
        notifiees_.pop_front();
        if (!waiter)
        {
            continue;
        }
        channel_.park(std::move(waiter));
        return;
    }
    stock_++;
}

void ConditionVariable::notify_all()
{
    broadcasting_.store(true, std::memory_order_release); // all incoming waits can return immediately
    {
        std::lock_guard lock(mutex_);
        auto end = std::remove_if(notifiees_.begin(), notifiees_.end(), [](const Async::Coroutine &coroutine) {
            return !coroutine;
        });
        if (end != notifiees_.begin())
        {
            channel_.park(notifiees_.begin(), end);
        }
        notifiees_.clear();
    }
    broadcasting_.store(false, std::memory_order_release);
}
} // namespace Foundation::NBIO