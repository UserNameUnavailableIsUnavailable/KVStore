#pragma once

#include <atomic>
#include <coroutine>
#include <functional>
#include <list>
#include <unordered_set>
#include <utility>
#include <vector>

#include "Coroutine.hpp"
#include "Multiplexer.hpp"

namespace Foundation::Async
{
template <typename T> class Task;

class Scheduler
{
  public:
    using Index = std::list<Coroutine>::iterator;

    explicit Scheduler(std::function<void(bool blocking)> idle) : idle_(std::move(idle))
    {
    }
    ~Scheduler() = default;

    Scheduler(const Scheduler &) = delete;
    Scheduler &operator=(const Scheduler &) = delete;

    // takes ownership of a root coroutine without starting it, and returns a
    // stable slot. Accepts any Task<T>: only the ownership base is stored.
    // Also allocates the control block (kept alive by the Coroutine node and
    // any CoroutineToken) and wires it to the promise for reclamation.
    template <typename T> Index prepare(Task<T> task)
    {
        // EnsureOwnerThread();
        auto handle = task.get_typed_handle();
        auto index = all_.insert(all_.end(), std::move(task)); // slices to base
        auto control_block = std::make_shared<CoroutineControlBlock>();
        control_block->root = index->get_handle();
        control_block->scheduler = this;
        control_block->index = index;
        handle.promise().control_block = control_block.get(); // non-owning
        // IMPORTANT: NEVER MOVE BEFORE get()!
        index->control_block() = std::move(control_block);
        return index;
    }

    // Fire-and-forget: take ownership of a root coroutine and queue its first
    // run (which happens on the next run() iteration, never inline here).
    // Returns a CoroutineToken so the caller can cancel / query the task.
    // The task runs to completion without anyone awaiting it; a non-void
    // result is simply dropped. Exceptions stay in the promise and are
    // currently discarded — the global exception sink is a follow-up.
    template <typename T> CoroutineToken spawn(Task<T> task)
    {
        auto index = prepare(std::move(task));
        submit(index->get_handle());
        return CoroutineToken{index->control_block()};
    }

    // Queues an already-suspended coroutine whose I/O became ready. Channels
    // call this instead of resuming inline, so user code never runs while a
    // channel member function is still on the stack.
    void submit(std::coroutine_handle<> handle)
    {
        // EnsureOwnerThread();
        ready_.push_back(handle);
    }

    // Called from a root coroutine's final_suspend. Only moves the node into
    // dead_ (O(1), no allocation, hence truly noexcept); the frame stays alive
    // until run() reclaims it.
    void complete(Index index) noexcept
    {
        dead_.splice(dead_.end(), all_, index);
    }

    // Marks a root coroutine for cancellation. The next run() iteration skips
    // resuming it and sweeps it from all_ into dead_ for reclamation. Routed
    // here by CoroutineToken::cancel().
    void cancel(std::coroutine_handle<> handle)
    {
        cancelled_.insert(handle);
    }

    // Marks EVERY live root for cancellation. Each root's teardown cascades to
    // its awaited children and detaches parked I/O, so after the next run()
    // sweep all_ is empty and Engine::run returns. The scheduler owns all_, so
    // callers need not track spawned tasks themselves. Safe to call from within
    // a running coroutine (it only marks; destruction happens at the sweep).
    void cancel_all()
    {
        for (const Coroutine &coroutine : all_)
        {
            cancelled_.insert(coroutine.get_handle());
        }
    }

    // runs one iteration: poll for events, run the coroutines that became ready
    // before this iteration, then reclaim finished ones.
    void run();

    // Whether any root coroutine is still alive; the driving loop (Engine::run)
    // exits when this becomes false.
    bool has_alive() const noexcept
    {
        return !all_.empty();
    }

  private:
    std::unique_ptr<Multiplexer> multiplexer_;
    std::list<Coroutine> all_;                              // all alive root coroutines
    std::vector<std::coroutine_handle<>> ready_;            // became ready, not yet run
    std::vector<std::coroutine_handle<>> batch_;            // swapped out of ready_ for this run
    std::unordered_set<std::coroutine_handle<>> cancelled_; // coroutines that have been cancelled
    std::list<Coroutine> dead_;                             // finished, awaiting reclamation
    std::function<void(bool blocking)> idle_;
    std::atomic_bool running_{false};
};
} // namespace Foundation::Async
