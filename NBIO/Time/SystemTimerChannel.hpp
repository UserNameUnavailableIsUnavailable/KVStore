#pragma once

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/PriorityQueue.hpp>
#include <NBIO/Time/SystemTimer.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <chrono>
#include <coroutine>
#include <queue>

namespace NBIO::Time {
namespace detail {
template <typename C>
class PollPayload {
   public:
    bool wants_poll() const noexcept { return !submitted_; }
    void take_poll() noexcept { submitted_ = true; }
    void release_poll() noexcept { submitted_ = false; }
    bool outstanding() const noexcept { return submitted_; }

   private:
    bool submitted_{false};
};
}  // namespace detail

class SystemTimerChannel;
namespace detail {

struct SystemTimerEntry {
    // std::coroutine_handle<> coroutine_view{};
    Async::Coroutine coroutine_view;
    std::chrono::steady_clock::time_point timepoint{};
};
// Stateless comparator functor: operator() can be inlined, avoiding the
// indirect-call overhead of a function pointer in priority_queue operations.
struct SystemTimerEntryComparator {
    bool operator()(const SystemTimerEntry& a, const SystemTimerEntry& b) const { return a.timepoint > b.timepoint; }
};
struct SleepAwaiter {
    SystemTimerChannel& channel;
    std::chrono::steady_clock::time_point due;

    SleepAwaiter(SystemTimerChannel& channel, std::chrono::steady_clock::time_point due) noexcept
        : channel(channel), due(due) {}

    SleepAwaiter(const SleepAwaiter&) = delete;
    SleepAwaiter& operator=(const SleepAwaiter&) = delete;

    ~SleepAwaiter() = default;

    bool await_ready() const noexcept { return due <= std::chrono::steady_clock::now(); }

    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType> handle);

    void await_resume() noexcept {}
};
}  // namespace detail

// SystemTimerChannel waits for a dedicated timer to fire.
// In the event handler, the channel pops several due entries, each entry containing a coroutine.
class SystemTimerChannel final : public NBIO::Core::Channel<SystemTimerChannel> {
    friend struct detail::SleepAwaiter;

   public:
    using Handle = Time::SystemTimer::Handle;
    using Payload = detail::PollPayload<SystemTimerChannel>;

    explicit SystemTimerChannel(NBIO::Time::SystemTimer& timer, NBIO::Core::Multiplexer& multiplexer,
                                NBIO::Async::Scheduler& scheduler);
    ~SystemTimerChannel() noexcept;

    NBIO::Time::SystemTimer& timer() noexcept { return timer_; }
    const NBIO::Time::SystemTimer& timer() const noexcept { return timer_; }

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& submit();
    void complete();

    // Sleep 原语：挂起当前协程直到 due，定时器触发后由调度器恢复。
    // 返回的 awaiter 直接 co_await 即可。
    detail::SleepAwaiter sleep(std::chrono::steady_clock::time_point due) noexcept {
        return detail::SleepAwaiter{*this, due};
    }

   private:
    void park(Async::Coroutine coroutine_view, std::chrono::steady_clock::time_point due);
    bool remove(Async::Coroutine coroutine_view);

    NBIO::Time::SystemTimer& timer_;
    std::queue<Async::Coroutine> immediate_queue_;  // already-timeout coroutines
    NBIO::Core::PriorityQueue<detail::SystemTimerEntry, detail::SystemTimerEntryComparator> queue_;
    Payload payload_{};
};

template <typename PromiseType>
inline void detail::SleepAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) {
    auto coroutine = Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine), due);
}
}  // namespace NBIO::Time




