#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Task.hpp>

#include <atomic>
#include <cassert>
#include <coroutine>
#include <deque>
#include <mutex>
#include <utility>

#include "NotifyChannel.hpp"
#include "Runtime.hpp"

namespace Foundation::NBIO
{
class ConditionVariable;

// Awaited by wait() (unconditionally) or wait(predicate). For the predicated
// form the predicate is re-evaluated under mutex_ in await_suspend -- the
// barrier that makes "check then park" atomic against notify_one/notify_all,
// so a notify that lands in the gap is never lost.
struct NoPredicate
{
    bool operator()() const noexcept
    {
        return false;
    }
};

template <typename Predicate = NoPredicate>
struct ConditionVariableAwaiter
{
    explicit ConditionVariableAwaiter(ConditionVariable &condition_variable, Predicate predicate = {}) noexcept
        : condition_variable(condition_variable), predicate(std::move(predicate))
    {
    }

    bool await_ready() noexcept;

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> h);

    void await_resume() noexcept
    {
    }

    ConditionVariable &condition_variable;
    Predicate predicate;
};

class ConditionVariable
{
  public:
    ConditionVariable();
    ~ConditionVariable() noexcept;

    ConditionVariable(const ConditionVariable &) = delete;
    ConditionVariable &operator=(const ConditionVariable &) = delete;

    // Wakes one waiter, if any.
    void notify_one();
    // Wakes all waiters.
    void notify_all();

    // Wait unconditionally.
    ConditionVariableAwaiter<> wait();
    // Wait until the predicate returns true.
    template <typename Predicate>
    Task<void> wait(Predicate predicate);

  private:
    template <typename Predicate>
    friend struct ConditionVariableAwaiter;

    std::mutex mutex_;
    NotifyChannel &channel_;
    std::deque<Async::Coroutine> notifiees_;
    std::size_t stock_{0};
    std::atomic_bool broadcasting_{false};
};

inline ConditionVariableAwaiter<> ConditionVariable::wait()
{
    return ConditionVariableAwaiter<>(*this);
}

template <typename Predicate>
inline Task<void> ConditionVariable::wait(Predicate predicate)
{
    // The awaiter re-checks the predicate under mutex_, so this loop only
    // absorbs spurious wake-ups.
    while (!predicate())
    {
        co_await ConditionVariableAwaiter<Predicate>{*this, predicate};
    }
}

template <typename Predicate>
bool ConditionVariableAwaiter<Predicate>::await_ready() noexcept
{
    // fast path when cv is broadcasting
    return condition_variable.broadcasting_.load(std::memory_order_acquire);
}

template <typename Predicate>
template <typename PromiseType>
bool ConditionVariableAwaiter<Predicate>::await_suspend(std::coroutine_handle<PromiseType> h)
{
    auto coroutine = Async::Coroutine::from_handle(h);

    {
        std::lock_guard lock(condition_variable.mutex_);

        // The barrier: re-evaluate the predicate under the lock notify_* also
        // take. A notify that has already run is visible here through the
        // mutex's happens-before edge, so we never sleep past it.
        if (predicate())
        {
            return false; // satisfied: resume inline, never register
        }

        if (condition_variable.stock_ > 0)
        {
            // A notification that arrived before this wait: nothing to sleep for.
            condition_variable.stock_--;
            return true;
        }

        condition_variable.notifiees_.push_back(std::move(coroutine));
    }

    // This wait is going to sleep, so from here until it is resumed a notification
    // has to be seen: the notifier prepares its read and arms for it. Arming belongs
    // here rather than in the notifier's constructor, which would have it watch a
    // condition variable nobody is waiting on, and rather than in park(), which runs
    // on whichever thread notifies and must not touch this engine's state.
    condition_variable.channel_.waiter_registered();
    return true;
}
} // namespace Foundation::NBIO
