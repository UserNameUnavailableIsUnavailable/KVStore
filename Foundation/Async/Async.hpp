#pragma once

#include <Foundation/Async/Task.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace Foundation::Async
{
namespace detail
{
// Wraps the root coroutine so a throw is captured and rethrown by run() after
// the loop drains, instead of propagating out of the scheduler.
template <typename RuntimeTag>
Task<RuntimeTag, void> guard_exception(Task<RuntimeTag, void> main, std::exception_ptr &exception)
{
    try
    {
        co_await std::move(main);
    }
    catch (...)
    {
        exception = std::current_exception();
    }
}
} // namespace detail

// Drives the scheduler until no root coroutine remains. The runtime must
// already be installed on this thread; the tag supplies its scheduler, so this
// knows nothing about any particular backend.
template <typename RuntimeTag> void run(Task<RuntimeTag, void> main)
{
    std::exception_ptr error;
    auto &scheduler = RuntimeTag::scheduler();
    scheduler.spawn(detail::guard_exception<RuntimeTag>(std::move(main), error));
    while (scheduler.is_runnable())
    {
        scheduler.run();
    }
    if (error)
    {
        std::rethrow_exception(error);
    }
}

// Fire-and-forget: takes ownership of a root coroutine on the tag's runtime.
template <typename RuntimeTag, typename T> CoroutineToken spawn(Task<RuntimeTag, T> task)
{
    return RuntimeTag::scheduler().spawn(std::move(task));
}

// Backend-provided behaviour (sleep, signal waiting, Net, IO) lives in the
// concrete backend namespace, e.g. Foundation::NBIO.

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
    kWaitAll,      // wait for every task, then rethrow the first error seen
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
struct CancelGuard
{
    std::vector<CoroutineToken> tokens;
    CancelGuard() = default;
    CancelGuard(const CancelGuard &) = delete;
    CancelGuard &operator=(const CancelGuard &) = delete;
    ~CancelGuard()
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
    // The awaiting frame plus its control block. The scheduler resumes by
    // reference, and the block is what makes the resume check possible.
    Coroutine continuation{};
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
                scheduler->submit(std::move(continuation));
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
    template <typename Promise> void await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        state->continuation = Coroutine::from_handle(handle);
    }
    void await_resume() const noexcept
    {
    }
};

template <typename RuntimeTag, typename T>
Task<RuntimeTag, void> run_all(Task<RuntimeTag, T> task, ResultSlot<T> &slot, std::shared_ptr<AllState> state)
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
    Coroutine continuation{};
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
            scheduler->submit(std::move(continuation));
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
    template <typename Promise> void await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        state->continuation = Coroutine::from_handle(handle);
    }
    std::size_t await_resume() const noexcept
    {
        return state->winner;
    }
};

template <typename RuntimeTag, typename T>
Task<RuntimeTag, void> run_any(Task<RuntimeTag, T> task, ResultSlot<T> &slot, std::shared_ptr<AnyState> state, std::size_t index)
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
// become std::monostate). Under kWaitAll (default) it waits for every task and
// then rethrows the first error seen; under kAbortOnError it rethrows as soon
// as any task throws, cancelling the rest.
template <WhenAllPolicy Policy = WhenAllPolicy::kWaitAll, typename RuntimeTag, typename... Ts>
Task<RuntimeTag, std::tuple<detail::WhenValue<Ts>...>> when_all(Task<RuntimeTag, Ts>... tasks)
{
    auto task_tuple = std::make_tuple(std::move(tasks)...);
    std::tuple<detail::ResultSlot<Ts>...> slots;

    auto state = std::make_shared<detail::AllState>();
    state->scheduler = &RuntimeTag::scheduler();
    state->remaining = sizeof...(Ts);
    state->abort_on_error = (Policy == WhenAllPolicy::kAbortOnError);

    detail::CancelGuard guard; // cancels any survivors on scope exit
    guard.tokens.reserve(sizeof...(Ts));

    [&]<std::size_t... I>(std::index_sequence<I...>) {
        (guard.tokens.push_back(spawn(detail::run_all(std::move(std::get<I>(task_tuple)), std::get<I>(slots), state))),
         ...);
    }(std::index_sequence_for<Ts...>{});

    co_await detail::AllAwaiter{state};

    // kWaitAll: all finished, first_error holds the first thrower (if any).
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
template <typename RuntimeTag, typename... Ts>
Task<RuntimeTag, std::variant<detail::WhenValue<Ts>...>> when_any(Task<RuntimeTag, Ts>... tasks)
{
    static_assert(sizeof...(Ts) > 0, "WhenAny requires at least one task");

    auto task_tuple = std::make_tuple(std::move(tasks)...);
    std::tuple<detail::ResultSlot<Ts>...> slots;

    auto state = std::make_shared<detail::AnyState>();
    state->scheduler = &RuntimeTag::scheduler();

    detail::CancelGuard guard; // cancels the losers on scope exit
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
