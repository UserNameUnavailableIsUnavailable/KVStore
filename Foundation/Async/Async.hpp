#pragma once

#include <Foundation/Async/Condition.hpp>
#include <Foundation/Async/Multiplexer.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "Engine.hpp"
#include "ListenService.hpp"
#include "Session.hpp"

namespace Foundation::Async
{
// Specify an underlying multiplexer for the current thread to use for coroutine framework.
// Note that this should be called before you run any Async function or create any Async object (e.g., Async::Condition), otherwise it raises a runtime error.
static inline void use_multiplexer(std::unique_ptr<Multiplexer> multiplexer)
{
    detail::Engine::use_multiplexer(std::move(multiplexer));
}
void run(Task<void> main);
template <typename T> CoroutineToken spawn(Task<T> task)
{
    return detail::Engine::spawn(std::move(task));
}

class Condition
{
public:
    Condition();
    ~Condition();
    void notify_one()
    {
        condition_.notify_one();
    }
    void notify_all()
    {
        condition_.notify_all();
    }
    using Awaiter = detail::ConditionAwaiter;
    Awaiter wait()
    {
        return condition_.wait();
    }
    template <typename Predicate>
    Task<void> wait(Predicate predicate)
    {
        return condition_.wait(predicate);
    }
private:
    detail::Condition condition_;
};

Task<void> sleep_until(std::chrono::steady_clock::time_point time_point);
Task<void> sleep_for(std::chrono::steady_clock::duration duration);

// Suspends until SIGINT/SIGTERM is delivered (via the engine's SignalChannel).
// The common shutdown idiom is: co_await WhenAny(AcceptLoop(), waitForSignal()).
Task<void> wait_for_signal();

namespace Net
{
std::unique_ptr<ListenService> listen(const Address &address, int backlog = 4096);
std::shared_ptr<Session> establish(Foundation::Socket socket);
} // namespace Net

// ============================================================================
// Structured concurrency combinators.
//
// A plain `co_await task` is sequential (the caller transfers into the callee).
// To run several tasks concurrently we first Spawn them all -- they become
// independent flows on the same scheduler -- and then wait on a shared
// completion signal:
//   * WhenAll: Spawn all, wait until all finish (or, under kAbortOnError, until
//     the first one throws), then return the results as a tuple.
//   * WhenAny: Spawn all, wait for the first to finish, then return its result
//     as a variant.
// In every case a cancelGuard cancels the remaining tasks when the combinator
// leaves scope -- normal return, thrown exception, or the combinator itself
// being cancelled -- so no spawned child is ever orphaned.
// ============================================================================

// Policy for how WhenAll reacts to a child throwing.
enum class WhenAllPolicy
{
    kwaitAll,      // wait for every task, then rethrow the first error seen
    kAbortOnError, // on the first error, cancel the rest and rethrow immediately
};

namespace detail
{
// void cannot be a tuple/variant element; map it to std::monostate.
template <typename T> using WhenValue = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

template <typename T> struct ResultSlot
{
    // index 0: unset, 1: value, 2: exception
    std::variant<std::monostate, WhenValue<T>, std::exception_ptr> value;

    WhenValue<T> take()
    {
        if (value.index() == 2)
        {
            std::rethrow_exception(std::get<2>(value));
        }
        return std::move(std::get<1>(value));
    }
};

// cancels every held token on destruction. cancel() on a finished token is a
// no-op, so this is safe on all exit paths (normal / throw / cancellation).
struct cancelGuard
{
    std::vector<CoroutineToken> tokens;
    cancelGuard() = default;
    cancelGuard(const cancelGuard &) = delete;
    cancelGuard &operator=(const cancelGuard &) = delete;
    ~cancelGuard()
    {
        for (auto &token : tokens)
        {
            token.cancel();
        }
    }
};

// ---- WhenAll -------------------------------------------------------------
struct AllState
{
    Scheduler *scheduler{nullptr};
    std::coroutine_handle<> continuation{};
    std::size_t remaining{0};
    std::exception_ptr first_error{};
    bool abort_on_error{false};
    bool woken{false};

    void OnDone(std::exception_ptr error)
    {
        if (error && !first_error)
        {
            first_error = error;
        }
        --remaining;
        const bool wake = (remaining == 0) || (abort_on_error && error);
        if (wake && !woken)
        {
            woken = true;
            if (continuation)
            {
                scheduler->submit(continuation);
            }
        }
    }
};

struct AllAwaiter
{
    std::shared_ptr<AllState> state;
    bool await_ready() const noexcept
    {
        return state->woken || state->remaining == 0;
    }
    void await_suspend(std::coroutine_handle<> handle) noexcept
    {
        state->continuation = handle;
    }
    void await_resume() const noexcept
    {
    }
};

template <typename T> Task<void> run_all(Task<T> task, ResultSlot<T> &slot, std::shared_ptr<AllState> state)
{
    std::exception_ptr error;
    try
    {
        if constexpr (std::is_void_v<T>)
        {
            co_await std::move(task);
            slot.value.template emplace<1>(std::monostate{});
        }
        else
        {
            slot.value.template emplace<1>(co_await std::move(task));
        }
    }
    catch (...)
    {
        error = std::current_exception();
        slot.value.template emplace<2>(error);
    }
    state->OnDone(error);
}

// ---- WhenAny -------------------------------------------------------------
struct AnyState
{
    Scheduler *scheduler{nullptr};
    std::coroutine_handle<> continuation{};
    std::size_t winner{static_cast<std::size_t>(-1)};
    bool fired{false};

    void complete(std::size_t index)
    {
        if (fired)
        {
            return; // a later finisher (racing loser) -- ignore
        }
        fired = true;
        winner = index;
        if (continuation)
        {
            scheduler->submit(continuation);
        }
    }
};

struct AnyAwaiter
{
    std::shared_ptr<AnyState> state;
    bool await_ready() const noexcept
    {
        return state->fired;
    }
    void await_suspend(std::coroutine_handle<> handle) noexcept
    {
        state->continuation = handle;
    }
    std::size_t await_resume() const noexcept
    {
        return state->winner;
    }
};

template <typename T>
Task<void> run_any(Task<T> task, ResultSlot<T> &slot, std::shared_ptr<AnyState> state, std::size_t index)
{
    try
    {
        if constexpr (std::is_void_v<T>)
        {
            co_await std::move(task);
            slot.value.template emplace<1>(std::monostate{});
        }
        else
        {
            slot.value.template emplace<1>(co_await std::move(task));
        }
    }
    catch (...)
    {
        slot.value.template emplace<2>(std::current_exception());
    }
    state->complete(index);
}

// Builds the result variant from the winner's slot (only the winner's take()
// runs, thanks to the per-index guard).
template <typename... Ts, typename Slots, std::size_t... I>
std::variant<WhenValue<Ts>...> collect_any(std::size_t winner, Slots &slots, std::index_sequence<I...>)
{
    std::variant<WhenValue<Ts>...> result;
    ((winner == I ? (void)(result.template emplace<I>(std::get<I>(slots).take())) : (void)0), ...);
    return result;
}
} // namespace detail

// Awaits all tasks concurrently. Returns their results as a tuple (void results
// become std::monostate). Under kwaitAll (default) it waits for every task and
// then rethrows the first error seen; under kAbortOnError it rethrows as soon
// as any task throws, cancelling the rest.
template <WhenAllPolicy Policy = WhenAllPolicy::kwaitAll, typename... Ts>
Task<std::tuple<detail::WhenValue<Ts>...>> when_all(Task<Ts>... tasks)
{
    auto task_tuple = std::make_tuple(std::move(tasks)...);
    std::tuple<detail::ResultSlot<Ts>...> slots;

    auto state = std::make_shared<detail::AllState>();
    state->scheduler = &detail::Engine::instance().scheduler();
    state->remaining = sizeof...(Ts);
    state->abort_on_error = (Policy == WhenAllPolicy::kAbortOnError);

    detail::cancelGuard guard; // cancels any survivors on scope exit
    guard.tokens.reserve(sizeof...(Ts));

    [&]<std::size_t... I>(std::index_sequence<I...>) {
        (guard.tokens.push_back(spawn(detail::run_all(std::move(std::get<I>(task_tuple)), std::get<I>(slots), state))),
         ...);
    }(std::index_sequence_for<Ts...>{});

    co_await detail::AllAwaiter{state};

    // kwaitAll: all finished, first_error holds the first thrower (if any).
    // kAbortOnError: woken early by a throw; guard cancels the survivors below.
    if (state->first_error)
    {
        std::rethrow_exception(state->first_error);
    }

    co_return std::apply([](auto &...slot) { return std::tuple<detail::WhenValue<Ts>...>{slot.take()...}; }, slots);
}

// Awaits all tasks concurrently; resumes as soon as the FIRST finishes and
// returns its result as a variant (void -> std::monostate). The remaining tasks
// are cancelled. If the winner threw, that exception is rethrown.
template <typename... Ts> Task<std::variant<detail::WhenValue<Ts>...>> when_any(Task<Ts>... tasks)
{
    static_assert(sizeof...(Ts) > 0, "WhenAny requires at least one task");

    auto task_tuple = std::make_tuple(std::move(tasks)...);
    std::tuple<detail::ResultSlot<Ts>...> slots;

    auto state = std::make_shared<detail::AnyState>();
    state->scheduler = &detail::Engine::instance().scheduler();

    detail::cancelGuard guard; // cancels the losers on scope exit
    guard.tokens.reserve(sizeof...(Ts));

    [&]<std::size_t... I>(std::index_sequence<I...>) {
        (guard.tokens.push_back(
             spawn(detail::run_any(std::move(std::get<I>(task_tuple)), std::get<I>(slots), state, I))),
         ...);
    }(std::index_sequence_for<Ts...>{});

    const std::size_t winner = co_await detail::AnyAwaiter{state};

    co_return detail::collect_any<Ts...>(winner, slots, std::index_sequence_for<Ts...>{});
}
} // namespace Foundation::Async
