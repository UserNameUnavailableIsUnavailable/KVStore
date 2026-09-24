#include "Scheduler.hpp"

#include <cassert>
#include <utility>

namespace Foundation::Async
{
void Scheduler::run()
{
    bool expected = false;
    // Re-entrancy guard: run() drives the loop and must never be nested.
    assert(running_.compare_exchange_strong(expected, true));

    reap_cancelled();

    // Park only when there is genuinely nothing to run and nothing to reclaim. A
    // pending cancellation counts as work: a frame is waiting to be destroyed
    // and the loop must not sleep through it.
    idle_(ready_.empty() && finished_.empty());

    assert(batch_.empty());
    std::swap(ready_, batch_);
    for (const Coroutine &coroutine : batch_)
    {
        if (!coroutine)
        {
            continue;
        }
        // is_dead() first: done() reads the frame, which may already be gone.
        if (coroutine.control_block->is_dead())
        {
            continue;
        }
        if (coroutine.handle.done())
        {
            continue;
        }
        coroutine.handle.resume();
    }
    batch_.clear();

    for (const auto &control_block : finished_)
    {
        if (control_block->join)
        {
            submit(std::move(control_block->join));
        }
    }

    for (auto &control_block : finished_)
    {
        control_block->reclaim();
        roots_.fetch_sub(1, std::memory_order_acq_rel);
    }
    finished_.clear();
    running_.store(false, std::memory_order_release);
}

void Scheduler::reap_cancelled()
{
    {
        std::lock_guard lock(cancel_mutex_);
        std::swap(cancel_batch_, cancelled_);
    }

    for (auto &ccb : cancel_batch_)
    {
        if (ccb->schedule_reclaim())
        {
            finished_.push_back(std::move(ccb));
        }
    }
}

void CoroutineToken::cancel()
{
    if (!coroutine_control_block_ || coroutine_control_block_->is_finished())
    {
        return;
    }
    coroutine_control_block_->scheduler->cancel(coroutine_control_block_);
}

bool CoroutineToken::is_finished() const noexcept
{
    return !coroutine_control_block_ || coroutine_control_block_->is_finished();
}
} // namespace Foundation::Async
