#pragma once

#include <nbio/async/Coroutine.hpp>
#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/utility/PriorityQueue.hpp>
#include <nbio/time/SystemTimer.hpp>
#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <chrono>
#include <coroutine>
#include <queue>

namespace nbio::time {
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
    async::Coroutine coroutine_view;
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
class SystemTimerChannel final : public nbio::core::Channel<SystemTimerChannel> {
    friend struct detail::SleepAwaiter;

   public:
    using Handle = time::SystemTimer::Handle;
    using Payload = detail::PollPayload<SystemTimerChannel>;

    explicit SystemTimerChannel(nbio::time::SystemTimer& timer, nbio::core::Multiplexer& multiplexer,
                                nbio::async::Scheduler& scheduler);
    ~SystemTimerChannel() noexcept;

    nbio::time::SystemTimer& timer() noexcept { return timer_; }
    const nbio::time::SystemTimer& timer() const noexcept { return timer_; }

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& Submit();
    void Complete();

    // Sleep 原语：挂起当前协程直到 due，定时器触发后由调度器恢复。
    // 返回的 awaiter 直接 co_await 即可。
    detail::SleepAwaiter sleep(std::chrono::steady_clock::time_point due) noexcept {
        return detail::SleepAwaiter{*this, due};
    }

   private:
    void Park(async::Coroutine coroutine_view, std::chrono::steady_clock::time_point due);
    bool remove(async::Coroutine coroutine_view);

    nbio::time::SystemTimer& timer_;
    std::queue<async::Coroutine> immediate_queue_;  // already-timeout coroutines
    nbio::core::PriorityQueue<detail::SystemTimerEntry, detail::SystemTimerEntryComparator> queue_;
    Payload payload_{};
};

template <typename PromiseType>
inline void detail::SleepAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) {
    auto coroutine = async::Coroutine::FromHandle(handle);
    channel.Arm();
    channel.Park(std::move(coroutine), due);
}
}  // namespace nbio::time




