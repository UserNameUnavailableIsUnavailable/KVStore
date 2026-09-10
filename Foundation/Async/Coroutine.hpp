#pragma once

#include <atomic>
#include <coroutine>
#include <list>
#include <memory>
#include <utility>

namespace Foundation::Async
{
class Scheduler;
class Coroutine;

// The terminal outcome a joiner observes via `co_await token`.
enum class JoinStatus
{
    kCompleted, // the task ran to normal completion
    kcancelled, // the task was cancelled before completing
};

struct CoroutineControlBlock
{
    std::coroutine_handle<> root{};
    Scheduler *scheduler{nullptr};
    std::atomic_bool cancelled{false};
    std::atomic_bool finished{false};
    std::list<Coroutine>::iterator
        index{}; // the index of this coroutine in the scheduler if it is not cancelled/finished
    std::coroutine_handle<> join_waiter{}; // a coroutine co_await-ing this task, woken on reclamation
};

class CoroutineToken
{
  public:
    CoroutineToken() noexcept = default;
    explicit CoroutineToken(std::shared_ptr<CoroutineControlBlock> control_block) noexcept
        : control_block_(std::move(control_block))
    {
    }

    // Request cancellation of the spawned task. Safe to call even after the
    // task has already finished (becomes a no-op): the control block outlives
    // the coroutine frame, so `finished` guards against a dangling `root`.
    void cancel();
    bool is_finished() const noexcept;

    // Join: suspend the caller until the task reaches a terminal state, then
    // report whether it completed normally or was cancelled. Reads the control
    // block (which outlives the frame), so it is safe even after reclamation.
    // Single joiner only. co_awaitable on an lvalue so the caller keeps the
    // token (e.g. to cancel() first, then await the outcome).
    auto operator co_await() const noexcept
    {
        struct JoinAwaiter
        {
            std::shared_ptr<CoroutineControlBlock> block;

            bool await_ready() const noexcept
            {
                return !block || block->finished.load(std::memory_order_acquire);
            }
            void await_suspend(std::coroutine_handle<> caller) noexcept
            {
                block->join_waiter = caller; // woken by the scheduler on reclamation
            }
            JoinStatus await_resume() const noexcept
            {
                return (block && block->cancelled.load(std::memory_order_acquire)) ? JoinStatus::kcancelled
                                                                                   : JoinStatus::kCompleted;
            }
        };
        return JoinAwaiter{control_block_};
    }

  private:
    std::shared_ptr<CoroutineControlBlock> control_block_; // the control block of the task to be cancelled/joined
};

class Coroutine
{
    friend class Scheduler;

  public:
    Coroutine() noexcept = default;
    explicit Coroutine(std::coroutine_handle<> handle) noexcept : handle_(handle)
    {
    }

    Coroutine(const Coroutine &) = delete;
    Coroutine &operator=(const Coroutine &) = delete;

    Coroutine(Coroutine &&other) noexcept
        : handle_(std::exchange(other.handle_, {})), control_block_(std::move(other.control_block_))
    {
    }

    Coroutine &operator=(Coroutine &&other) noexcept
    {
        if (this != &other)
        {
            if (handle_)
            {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
            control_block_ = std::move(other.control_block_);
        }
        return *this;
    }

    ~Coroutine() noexcept
    {
        if (control_block_)
        {
            control_block_->finished.store(true, std::memory_order_release);
        }
        if (handle_)
        {
            handle_.destroy();
        }
    }

    std::coroutine_handle<> get_handle() const noexcept
    {
        return handle_;
    }
    void set_handle(std::coroutine_handle<> handle) noexcept
    {
        auto old = std::exchange(handle_, handle);
        if (old)
            old.destroy();
    }
    std::coroutine_handle<> release_handle() noexcept
    {
        return std::exchange(handle_, {});
    }

    bool finished() const noexcept
    {
        return !handle_ || handle_.done();
    }

    // For a root coroutine that nobody co_awaits: start it running explicitly.
    void run()
    {
        if (handle_ && !handle_.done())
        {
            handle_.resume();
        }
    }

  protected:
    const std::shared_ptr<CoroutineControlBlock> &control_block() const noexcept
    {
        return control_block_;
    }
    std::shared_ptr<CoroutineControlBlock> &control_block() noexcept
    {
        return control_block_;
    }

    std::coroutine_handle<> handle_{};
    std::shared_ptr<CoroutineControlBlock> control_block_{};
};
} // namespace Foundation::Async
