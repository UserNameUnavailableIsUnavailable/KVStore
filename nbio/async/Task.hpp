#pragma once

#include <cassert>
#include <concepts>
#include <coroutine>
#include <exception>
#include <utility>
#include <variant>

#include "Coroutine.hpp"
#include "Scheduler.hpp"

namespace nbio::async {
class Scheduler;

// IMPORTANT: this must only queue the frame for reclamation. It must NEVER
// destroy the frame: it is called from final_suspend, where the FinalAwaiter
// object itself still lives inside that very frame.
struct Promise {
    // Idiomatic symmetric transfer.
    // Coroutines can be chained like: root -> children.
    // A descendant has to return control back to its ancestor so the ancestor can continue from where it suspends. This
    // is called symmetric transfer.
    struct FinalAwaiter;

    // The timing of initial_suspend():
    // 1. create a frame;
    // 2. copies the parameters into the frame;
    // 3. construct the promise object;
    // 4. calls `initial_suspend()`;
    // 5. if `initial_suspend()` returns `suspend_never`, the coroutine body is run immediately even if it is not
    // awaited; if it returns `suspend_always`, the Task object will be returned to the caller. CAVEAT: The caller must
    // explicitly run a coroutine.

    // Error-prone code:
    // Something* something_ptr{ nullptr };
    // auto task = CreateTask(something_ptr); // runs immediately if we return `suspend_never`
    // auto something = GetSomething();
    // something_ptr = &something;

    // LAZY: the coroutine body does not run until the Task is explicitly `co_await`ed.
    // This guarantees the Task object fully constructed before any body
    // code runs, so exceptions and lifetime have a well-defined home.
    std::suspend_always initial_suspend() noexcept { return {}; }

    // `final_suspend` controls the behavior when the current coroutine exits.
    // By returning a FinalAwaiter, we transfer the control back to the descendant's ancestor.
    FinalAwaiter final_suspend() noexcept { return {}; }

    std::coroutine_handle<> continuation{};  // The descendant

    // The root coroutine holds a non-empty control block and is responsible for `finish()` the coroutine before it
    // exits. `control_block->finish();` schedules a Reclaimation of the coroutine chain. ALL descendants have empty
    // control block pointer.
    CoroutineControlBlock* control_block = nullptr;  // The control block of the entire coroutine chain.

    struct FinalAwaiter {
        bool await_ready() noexcept { return false; }

        template <typename PromiseType>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseType> me) noexcept {
            auto& promise = me.promise();
            auto continuation = promise.continuation;
            auto* control_block = promise.control_block;

            // identify whether I AM the root coroutine
            if (!continuation && control_block != nullptr && control_block->root.address() == me.address()) {
                // The chain is over. This is the notification the scheduler waits for; it
                // only results the block out of the registry (see `Scheduler::Reclaim()`), it
                // does not destroy the frame at once -- this very frame is kept alive until the last one holding the
                // control block releases it. This avoids UAF.
                control_block->Finish();
            }
            // If there is no continuation, transfer to noop_coroutine, which releases everything properly for us.
            return continuation ? continuation : std::noop_coroutine();
        }
        void await_resume() noexcept {}
    };
};

template <typename runtimeTag, typename T>
class Task {
   public:
    struct promise_type : Promise {
        // Names the runtime that owns this frame. The awaiter compares it with
        // the caller's, so a cross-runtime co_await is a compile error rather
        // than the callee running on the wrong thread.
        using runtime = runtimeTag;

        std::variant<std::monostate, T, std::exception_ptr> result_;

        Task get_return_object() noexcept { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

        void return_value(T value) { result_.template emplace<1>(std::move(value)); }

        void unhandled_exception() noexcept { result_.template emplace<2>(std::current_exception()); }

        T result() {
            if (result_.index() == 2) {
                std::rethrow_exception(std::get<2>(result_));
            }
            assert(result_.index() == 1 && "coroutine finished without a value");
            return std::move(std::get<1>(result_));
        }
    };

    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~Task() noexcept {
        if (handle_) {
            handle_.destroy();
        }
    }

    // Recovers the typed handle from the erased one; no second handle is stored.
    std::coroutine_handle<promise_type> get_typed_handle() const noexcept {
        return std::coroutine_handle<promise_type>::from_address(handle_.address());
    }

    // Relinquish the frame without destroying it: the ownership handover to a
    // scheduler. Afterwards this Task is empty and its destructor is a no-op.
    std::coroutine_handle<> release_handle() noexcept { return std::exchange(handle_, {}); }

    // A Task can be awaited exactly once (afterwards its result has been moved
    // out and the frame sits at final suspend). The && qualifier makes
    // `Task t = f(); co_await t;` fail to compile, forcing `co_await f()` or
    // `co_await std::move(t)`.
    //
    // Awaiter is a nested type rather than a local one: its await_suspend is a
    // member template, and local classes cannot declare templates.
    struct Awaiter {
        std::coroutine_handle<promise_type> callee_;

        bool await_ready() noexcept { return !callee_ || callee_.done(); }

        // Only a caller belonging to the same runtime may await this task.
        template <typename CallerPromise>
            requires std::same_as<typename CallerPromise::runtime, runtimeTag>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<CallerPromise> caller) noexcept {
            auto& callee = callee_.promise();
            callee.continuation = caller;
            // Inherit the root's control block. The whole await tree shares one,
            // which is what makes a cancel at the root visible to a descendant
            // Parked deep in the chain. Spawn() means "independent root", so it
            // never inherits.
            callee.control_block = caller.promise().control_block;
            return callee_;  // symmetric transfer into the callee
        }

        T await_resume() { return callee_.promise().result(); }
    };

    // Task is non-reentrant, meaning it is designed for single-use, so it needs to be `&&`.
    // Imagine if a lvalue is allowed, then the user is allowed to do something like this:
    // auto task = CreateTask();
    // co_await task; // the coroutine frame will be destroyed
    // co_await task; // use after free!
    Awaiter operator co_await() && noexcept { return Awaiter{get_typed_handle()}; }

   private:
    std::coroutine_handle<> handle_{};
};

template <typename runtimeTag>
class Task<runtimeTag, void> {
   public:
    struct promise_type : Promise {
        using runtime = runtimeTag;

        std::exception_ptr error_;

        Task get_return_object() noexcept { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

        void return_void() noexcept {}

        void unhandled_exception() noexcept { error_ = std::current_exception(); }

        void result() {
            if (error_) {
                std::rethrow_exception(error_);
            }
        }
    };

    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_) {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~Task() noexcept {
        if (handle_) {
            handle_.destroy();
        }
    }

    std::coroutine_handle<promise_type> get_typed_handle() const noexcept {
        return std::coroutine_handle<promise_type>::from_address(handle_.address());
    }

    std::coroutine_handle<> release_handle() noexcept { return std::exchange(handle_, {}); }

    struct Awaiter {
        std::coroutine_handle<promise_type> callee_;

        bool await_ready() noexcept { return !callee_ || callee_.done(); }

        template <typename CallerPromise>
            requires std::same_as<typename CallerPromise::runtime, runtimeTag>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<CallerPromise> caller) noexcept {
            // in side a symmetric transfer, the callee inherits the caller's control block and continuation.
            auto& callee = callee_.promise();
            callee.control_block = caller.promise().control_block;  // inherit the tree's block
            callee.continuation = caller;
            return callee_;
        }

        void await_resume() { callee_.promise().result(); }
    };

    Awaiter operator co_await() && noexcept { return Awaiter{get_typed_handle()}; }

   private:
    std::coroutine_handle<> handle_{};
};
}  // namespace nbio::async
