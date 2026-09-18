#pragma once

#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdio>
#include <execinfo.h>
#include <memory>
#include <unistd.h>
#include <utility>

namespace Foundation::Async
{
class Scheduler;

struct CoroutineControlBlock;

struct Coroutine
{
    std::coroutine_handle<> handle{};
    std::shared_ptr<CoroutineControlBlock> control_block{};

    explicit operator bool() const noexcept;
    bool operator==(const Coroutine &other) const noexcept
    {
        return other.handle == handle;
    }

    template <typename Promise>
    static Coroutine from_handle(std::coroutine_handle<Promise> handle) noexcept
    {
        // TEMPORARY: which block does this handle think it has?
        // std::fprintf(stderr, "from_handle handle=%p block=%p\n", handle.address(),
        //              static_cast<const void *>(handle.promise().control_block));
        return Coroutine{handle, handle.promise().control_block->shared_from_this()};
    }
};

struct CoroutineControlBlock : std::enable_shared_from_this<CoroutineControlBlock>
{
    std::coroutine_handle<> root{}; // root coroutine's handle
    Scheduler *scheduler{nullptr};
    std::atomic_bool cancelled{false};
    std::atomic_bool finished{false};
    bool reclaim_queued{false};
    Coroutine join{};

    bool is_cancelled() const noexcept
    {
        return cancelled.load(std::memory_order_acquire);
    }
    bool is_finished() const noexcept
    {
        return finished.load(std::memory_order_acquire);
    }
    bool is_dead() const noexcept
    {
        return is_cancelled() || is_finished();
    }

    void cancel() noexcept
    {
        cancelled.store(true, std::memory_order_release);
    }

    bool schedule_reclaim() noexcept
    {
        if (reclaim_queued)
        {
            return false;
        }
        reclaim_queued = true;
        return true;
    }

    void reclaim() noexcept
    {
        finished.store(true, std::memory_order_release);
        if (auto h = std::exchange(root, {}))
        {
            h.destroy();
        }
    }

    ~CoroutineControlBlock() noexcept
    {
        assert(!root && "control block leaked: the scheduler never reclaimed it");
        if (auto h = std::exchange(root, {}))
        {
            h.destroy();
        }
    }
};

// The outcome a joiner observes via `co_await token`.
enum class JoinStatus
{
    kCompleted, // the task ran to normal completion
    kCancelled, // the task was cancelled before completing
};

class CoroutineToken
{
  public:
    CoroutineToken() noexcept = default;
    explicit CoroutineToken(std::shared_ptr<CoroutineControlBlock> control_block) noexcept
        : coroutine_control_block_(std::move(control_block))
    {
    }

    void cancel();
    bool is_finished() const noexcept;

    struct JoinAwaiter
    {
        std::shared_ptr<CoroutineControlBlock> block;

        bool await_ready() const noexcept
        {
            return !block || block->is_finished();
        }
        template <typename Promise> void await_suspend(std::coroutine_handle<Promise> caller) noexcept
        {
            block->join = Coroutine::from_handle(caller); // woken by the scheduler on reclamation
        }
        JoinStatus await_resume() const noexcept
        {
            return (block && block->is_cancelled()) ? JoinStatus::kCancelled : JoinStatus::kCompleted;
        }
    };

    JoinAwaiter operator co_await() const noexcept
    {
        return JoinAwaiter{coroutine_control_block_};
    }

  private:
    std::shared_ptr<CoroutineControlBlock> coroutine_control_block_;
};

inline Coroutine::operator bool() const noexcept
{
    // The order matters, a handle dangles if the root coroutine is cancelled.
    return static_cast<bool>(handle) && !control_block->is_dead() && !handle.done();
}

} // namespace Foundation::Async
