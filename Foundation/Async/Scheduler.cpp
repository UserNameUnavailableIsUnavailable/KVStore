#include "Scheduler.hpp"

#include <atomic>
#include <cassert>

namespace Foundation::Async
{
void Scheduler::run()
{
    bool expected = false;
    // if assertion fails, then run() must be called from different threads at the same time
    assert(running_.compare_exchange_strong(expected, true));

    // Block only when there is nothing to run; otherwise just harvest events.
    // A pending cancellation also counts as work, so we do not park in epoll
    // while a coroutine is waiting to be reclaimed.
    idle_(ready_.empty() && cancelled_.empty());

    // Swap so coroutines that become ready during this batch run in the next
    // iteration: that keeps each iteration bounded and round-robin fair.
    std::swap(ready_, batch_); // FIXME: not thread-safe
    for (auto handle : batch_)
    {
        // Only check validity BEFORE resuming: after resume() the handle may
        // already have been destroyed (an awaited Task is a temporary owned
        // by its caller's frame).
        if (handle && !handle.done() && !cancelled_.contains(handle))
        {
            handle.resume();
        }
    }
    auto end = std::remove_if(all_.begin(), all_.end(), [this](const Coroutine &coroutine) {
        return cancelled_.contains(coroutine.get_handle());
    });
    if (end != all_.end())
    {
        dead_.splice(dead_.end(), all_, end, all_.end());
    }

    cancelled_.clear(); // FIXME: not thread-safe

    batch_.clear();

    // Wake any coroutine joining a task that is about to be reclaimed. Both
    // terminal paths funnel through dead_ (normal completion via Complete(),
    // cancellation via the sweep above), so this single point covers both.
    // The join awaiter reads the control block -- which outlives the frame --
    // so it is safe even though dead_.clear() destroys the frame next.
    for (const Coroutine &coroutine : dead_)
    {
        const auto &control_block = coroutine.control_block();
        if (control_block && control_block->join_waiter)
        {
            submit(control_block->join_waiter);
        }
    }

    // The single destruction point. Safe here: no coroutine is running and
    // no channel member function is on the stack.
    dead_.clear();
    running_.store(false, std::memory_order_release);
}

void CoroutineToken::cancel()
{
    if (!control_block_)
    {
        return;
    }
    // Already reclaimed: `root` is dangling, so must not touch the scheduler.
    if (control_block_->finished.load(std::memory_order_acquire))
    {
        return;
    }
    // Idempotent: only the first cancel() forwards to the scheduler.
    if (control_block_->cancelled.exchange(true))
    {
        return;
    }
    control_block_->scheduler->cancel(control_block_->root);
}

bool CoroutineToken::is_finished() const noexcept
{
    return !control_block_ || control_block_->finished.load(std::memory_order_acquire);
}
} // namespace Foundation::Async
