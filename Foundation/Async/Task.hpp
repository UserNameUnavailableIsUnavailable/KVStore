#pragma once

#include <cassert>
#include <concepts>
#include <coroutine>
#include <exception>
#include <utility>
#include <variant>

#include "Coroutine.hpp"
#include "Scheduler.hpp"

namespace Foundation::Async
{
class Scheduler;

// IMPORTANT: this must only queue the frame for reclamation. It must NEVER
// destroy the frame: it is called from final_suspend, where the FinalAwaiter
// object itself still lives inside that very frame.
struct Promise
{
    struct FinalAwaiter
    {
        bool await_ready() noexcept
        {
            return false;
        }
        // Returns the continuation instead of calling .resume() on it, so the
        // compiler emits a tail call: the finishing coroutine's frame is popped
        // before jumping into the awaiter. Without this, a long await-chain (or
        // a loop that keeps awaiting a ready value) grows the native stack.
        template <typename PromiseType>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseType> me) noexcept
        {
            auto &promise = me.promise();
            auto continuation = promise.continuation;
            // No continuation means this frame is the root of its tree. Children
            // always have one, and since the block is propagated down the await
            // chain they carry a non-null block too -- so identity, not the
            // presence of a block, is what distinguishes a root here.
            auto *control_block = promise.control_block;
            if (!continuation && control_block != nullptr && control_block->root.address() == me.address())
            {
                control_block->scheduler->finish(control_block->shared_from_this());
            }
            return continuation ? continuation : std::noop_coroutine();
        }
        void await_resume() noexcept
        {
        }
    };

    // Lazy: the coroutine body does not run until the Task is co_awaited (or
    // run()ed). This keeps the Task object fully constructed before any body
    // code runs, so exceptions and lifetime have a well-defined home.
    std::suspend_always initial_suspend() noexcept
    {
        return {};
    }

    FinalAwaiter final_suspend() noexcept
    {
        return {};
    }

    std::coroutine_handle<> continuation;
    // Non-owning. Names the control block of the tree this frame belongs to:
    // set on a root by spawn(), and inherited from the caller by every awaited
    // child. Non-null on every schedulable frame, which is what lets a parked
    // descendant read its tree's cancellation state.
    CoroutineControlBlock *control_block = nullptr;
};

template <typename RuntimeTag, typename T> class Task
{
  public:
    struct promise_type : Promise
    {
        // Names the runtime that owns this frame. The awaiter compares it with
        // the caller's, so a cross-runtime co_await is a compile error rather
        // than the callee running on the wrong thread.
        using Runtime = RuntimeTag;

        std::variant<std::monostate, T, std::exception_ptr> result_;

        Task get_return_object() noexcept
        {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_value(T value)
        {
            result_.template emplace<1>(std::move(value));
        }

        void unhandled_exception() noexcept
        {
            result_.template emplace<2>(std::current_exception());
        }

        T take()
        {
            if (result_.index() == 2)
            {
                std::rethrow_exception(std::get<2>(result_));
            }
            assert(result_.index() == 1 && "coroutine finished without a value");
            return std::move(std::get<1>(result_));
        }
    };

    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle)
    {
    }

    Task(const Task &) = delete;
    Task &operator=(const Task &) = delete;

    Task(Task &&other) noexcept : handle_(std::exchange(other.handle_, {}))
    {
    }

    Task &operator=(Task &&other) noexcept
    {
        if (this != &other)
        {
            if (handle_)
            {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~Task() noexcept
    {
        if (handle_)
        {
            handle_.destroy();
        }
    }

    // Recovers the typed handle from the erased one; no second handle is stored.
    std::coroutine_handle<promise_type> get_typed_handle() const noexcept
    {
        return std::coroutine_handle<promise_type>::from_address(handle_.address());
    }

    // Relinquish the frame without destroying it: the ownership handover to a
    // scheduler. Afterwards this Task is empty and its destructor is a no-op.
    std::coroutine_handle<> release_handle() noexcept
    {
        return std::exchange(handle_, {});
    }

    // A Task can be awaited exactly once (afterwards its result has been moved
    // out and the frame sits at final suspend). The && qualifier makes
    // `Task t = f(); co_await t;` fail to compile, forcing `co_await f()` or
    // `co_await std::move(t)`.
    //
    // Awaiter is a nested type rather than a local one: its await_suspend is a
    // member template, and local classes cannot declare templates.
    struct Awaiter
    {
        std::coroutine_handle<promise_type> callee_;

        bool await_ready() noexcept
        {
            return !callee_ || callee_.done();
        }

        // Only a caller belonging to the same runtime may await this task.
        template <typename CallerPromise>
            requires std::same_as<typename CallerPromise::Runtime, RuntimeTag>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<CallerPromise> caller) noexcept
        {
            auto &callee = callee_.promise();
            callee.continuation = caller;
            // Inherit the root's control block. The whole await tree shares one,
            // which is what makes a cancel at the root visible to a descendant
            // parked deep in the chain. spawn() means "independent root", so it
            // never inherits.
            callee.control_block = caller.promise().control_block;
            return callee_; // symmetric transfer into the callee
        }

        T await_resume()
        {
            return callee_.promise().take();
        }
    };

    Awaiter operator co_await() && noexcept
    {
        return Awaiter{get_typed_handle()};
    }

  private:
    std::coroutine_handle<> handle_{};
};

template <typename RuntimeTag> class Task<RuntimeTag, void>
{
  public:
    struct promise_type : Promise
    {
        using Runtime = RuntimeTag;

        std::exception_ptr error_;

        Task get_return_object() noexcept
        {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_void() noexcept
        {
        }

        void unhandled_exception() noexcept
        {
            error_ = std::current_exception();
        }

        void take()
        {
            if (error_)
            {
                std::rethrow_exception(error_);
            }
        }
    };

    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle)
    {
    }

    Task(const Task &) = delete;
    Task &operator=(const Task &) = delete;

    Task(Task &&other) noexcept : handle_(std::exchange(other.handle_, {}))
    {
    }

    Task &operator=(Task &&other) noexcept
    {
        if (this != &other)
        {
            if (handle_)
            {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~Task() noexcept
    {
        if (handle_)
        {
            handle_.destroy();
        }
    }

    std::coroutine_handle<promise_type> get_typed_handle() const noexcept
    {
        return std::coroutine_handle<promise_type>::from_address(handle_.address());
    }

    std::coroutine_handle<> release_handle() noexcept
    {
        return std::exchange(handle_, {});
    }

    struct Awaiter
    {
        std::coroutine_handle<promise_type> callee_;

        bool await_ready() noexcept
        {
            return !callee_ || callee_.done();
        }

        template <typename CallerPromise>
            requires std::same_as<typename CallerPromise::Runtime, RuntimeTag>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<CallerPromise> caller) noexcept
        {
            // in side a symmetric transfer, the callee inherits the caller's control block and continuation.
            auto &callee = callee_.promise();
            callee.control_block = caller.promise().control_block; // inherit the tree's block
            callee.continuation = caller;
            return callee_;
        }

        void await_resume()
        {
            callee_.promise().take();
        }
    };

    Awaiter operator co_await() && noexcept
    {
        return Awaiter{get_typed_handle()};
    }

  private:
    std::coroutine_handle<> handle_{};
};
} // namespace Foundation::Async
