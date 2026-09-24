#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "Coroutine.hpp"

namespace Foundation::Async
{
template <typename RuntimeTag, typename T> class Task;

// Owns the coroutines that are ready to run, and destroys the frames that have
// reached a terminal state.
//
// A scheduler owns no registry of the tasks it admitted. A live root is always
// referenced from wherever it is parked -- a channel's waiter slot, the ready
// queue, the reclaim queue -- because every one of those holds a
// PendingCoroutine. That is why the control block can own the frame, and why
// nothing has to be swept to find out what is still alive.
class Scheduler
{
  public:
    explicit Scheduler(std::function<void(bool blocking)> idle) : idle_(std::move(idle))
    {
    }
    ~Scheduler() = default;

    Scheduler(const Scheduler &) = delete;
    Scheduler &operator=(const Scheduler &) = delete;

    // Takes ownership of a root coroutine and queues its first run, which
    // happens in the next run() iteration and never inline here. Returns a token
    // so the caller can cancel or join it.
    //
    // This is the ownership handover: the Task gives up its frame, the control
    // block takes it, and from here the frame lives exactly as long as some
    // queue or token references the block.
    template <typename RuntimeTag, typename T> CoroutineToken spawn(Task<RuntimeTag, T> task)
    {
        auto handle = task.get_typed_handle();
        auto control_block = std::make_shared<CoroutineControlBlock>();
        control_block->root = task.release_handle();
        control_block->scheduler = this;
        handle.promise().control_block = control_block.get(); // non-owning
        ready_.push_back(Coroutine{handle, control_block});
        roots_.fetch_add(1, std::memory_order_acq_rel);
        return CoroutineToken{std::move(control_block)};
    }

    void submit(Coroutine pending)
    {
        ready_.push_back(std::move(pending));
    }

    void finish(const std::shared_ptr<CoroutineControlBlock> &control_block) noexcept
    {
        if (control_block->schedule_reclaim())
        {
            finished_.push_back(control_block);
        }
    }

    void cancel(const std::shared_ptr<CoroutineControlBlock> &control_block) noexcept
    {
        if (!control_block)
        {
            return;
        }
        control_block->cancel();
        std::lock_guard lock(cancel_mutex_);
        cancelled_.push_back(control_block);
    }

    void run();

    bool is_runnable() const noexcept
    {
        // A scheduler has work as long as any root is alive, including one
        // parked in a channel with nothing in ready_/finished_/cancelled_. Only
        // a live-root count sees that, which is why it drops at reclamation and
        // not at completion.
        return roots_.load(std::memory_order_acquire) > 0;
    }

  private:
    // Moves cross-thread cancellations into reclaim_, on the owning thread.
    void reap_cancelled();

    std::vector<Coroutine> ready_;                     // ready, not yet run
    std::vector<Coroutine> batch_; // a coroutine resumed may submit a new coroutine into ready_, so we swap ready_ into batch_ before each run
    std::atomic_size_t roots_{0};                      // the number of all spawned coroutines (root coroutines are always created by spawn())
    std::vector<std::shared_ptr<CoroutineControlBlock>> finished_; // terminal, awaiting destruction
    std::vector<std::shared_ptr<CoroutineControlBlock>> cancelled_; // cancellation requests, any thread
    std::vector<std::shared_ptr<CoroutineControlBlock>> cancel_batch_; // cancellation requests, any thread
    mutable std::mutex cancel_mutex_;                                   // guards cancelled_
    std::function<void(bool blocking)> idle_;                 // called when no coroutine ready
    std::atomic_bool running_{false};
};
} // namespace Foundation::Async
