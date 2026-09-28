#pragma once

#include <atomic>
#include <cassert>
#include <functional>
#include <list>
#include <memory>
#include <utility>
#include <vector>

#include "Coroutine.hpp"

namespace Foundation::Async
{
template <typename RuntimeTag, typename T> class Task;

// Drives the coroutines of one thread, and destroys the frames that have reached a
// terminal state. One thread owns a Scheduler: spawn(), submit() and cancel() all come
// from coroutines it is running.
class Scheduler
{
  public:
    explicit Scheduler(std::function<void(bool blocking)> idle) : idle_(std::move(idle))
    {
    }
    ~Scheduler() = default;

    Scheduler(const Scheduler &) = delete;
    Scheduler &operator=(const Scheduler &) = delete;

    // Takes ownership of a new root coroutine and queues its first run. The block goes
    // into the registry, and that registry entry is the only strong reference to it: the
    // token this returns observes and never owns.
    template <typename RuntimeTag, typename T>
    CoroutineToken spawn(Task<RuntimeTag, T> task)
    {
        auto handle = task.get_typed_handle();
        auto control_block = std::make_shared<CoroutineControlBlock>();
        control_block->root = task.release_handle();
        control_block->scheduler = this;
        // What tells a frame whether it is the root of its tree, and lets any frame of
        // the tree name the block it belongs to. Non-owning: the registry owns.
        handle.promise().control_block = control_block.get();
        roots_.push_back(control_block);
        control_block->index = std::prev(roots_.end());
        ready_.push_back(Coroutine{handle, control_block});
        return CoroutineToken{control_block};
    }

    void submit(Coroutine pending)
    {
        ready_.push_back(std::move(pending));
    }

    // Takes a block out of the registry.
    void reclaim(const CoroutineControlBlock &ccb) noexcept;

    void run();

    bool is_runnable() const noexcept
    {
        return !roots_.empty() || !reclaimed_.empty();
    }

  private:
    std::vector<Coroutine> ready_;
    std::vector<Coroutine> ready_batch_; // a coroutine resumed may submit a new coroutine into ready_, so we swap ready_ into batch_ before each run

    std::list<std::shared_ptr<CoroutineControlBlock>> roots_;
    std::list<std::shared_ptr<CoroutineControlBlock>> reclaimed_;

    std::function<void(bool blocking)> idle_; // called when no coroutine ready
    std::atomic_bool running_{false};
};
} // namespace Foundation::Async
