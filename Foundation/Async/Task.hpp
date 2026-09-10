#pragma once

#include <cassert>
#include <coroutine>
#include <exception>
#include <utility>
#include <variant>

#include "Coroutine.hpp"
#include "Scheduler.hpp"

namespace Foundation::Async
{
class Scheduler;

// IMPORTANT: this must only move the task into the scheduler's dead list. It
// must NEVER destroy the frame: it is called from final_suspend, where the
// FinalAwaiter object itself still lives inside that very frame.
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
            // No continuation => this is a scheduler-owned root coroutine that
            // just finished. Notify the owner (via the control block) so it
            // reclaims the frame later.
            if (!continuation && promise.control_block != nullptr)
            {
                promise.control_block->scheduler->complete(promise.control_block->index);
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
    // Non-owning: points at the control block owned by the scheduler-side
    // Coroutine (and the CoroutineToken). Only set for root coroutines; null
    // for awaited children. Used by FinalAwaiter to trigger reclamation.
    CoroutineControlBlock *control_block = nullptr;
};

template <typename T> class Task : public Coroutine
{
  public:
    struct promise_type : Promise
    {
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

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : Coroutine(handle)
    {
    }

    // Recovers the typed handle from the erased one; no second handle is stored.
    std::coroutine_handle<promise_type> get_typed_handle() const noexcept
    {
        return std::coroutine_handle<promise_type>::from_address(handle_.address());
    }

    // A Task can be awaited exactly once (afterwards its result has been moved
    // out and the frame sits at final suspend). The && qualifier makes
    // `Task t = f(); co_await t;` fail to compile, forcing `co_await f()` or
    // `co_await std::move(t)`.
    auto operator co_await() && noexcept
    {
        struct Awaiter
        {
            std::coroutine_handle<promise_type> callee_;

            bool await_ready() noexcept
            {
                return !callee_ || callee_.done();
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept
            {
                callee_.promise().continuation = caller;
                return callee_; // symmetric transfer into the callee
            }

            T await_resume()
            {
                return callee_.promise().take();
            }
        };
        return Awaiter{get_typed_handle()};
    }
};

template <> class Task<void> : public Coroutine
{
  public:
    struct promise_type : Promise
    {
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

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : Coroutine(handle)
    {
    }

    std::coroutine_handle<promise_type> get_typed_handle() const noexcept
    {
        return std::coroutine_handle<promise_type>::from_address(handle_.address());
    }

    auto operator co_await() && noexcept
    {
        struct Awaiter
        {
            std::coroutine_handle<promise_type> callee_;

            bool await_ready() noexcept
            {
                return !callee_ || callee_.done();
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept
            {
                callee_.promise().continuation = caller;
                return callee_;
            }

            void await_resume()
            {
                callee_.promise().take();
            }
        };
        return Awaiter{get_typed_handle()};
    }
};
} // namespace Foundation::Async
