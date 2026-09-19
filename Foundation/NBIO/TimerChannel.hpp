#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/PriorityQueue.hpp>
#include <Foundation/Core/Timer.hpp>
#include <chrono>
#include <coroutine>
#include <Foundation/Async/Coroutine.hpp>

namespace Foundation::NBIO
{
class TimerChannel;
namespace detail
{

struct TimerEntry
{
    // std::coroutine_handle<> coroutine_view{};
    Async::Coroutine coroutine_view;
    std::chrono::steady_clock::time_point timepoint{};
};
// Stateless comparator functor: operator() can be inlined, avoiding the
// indirect-call overhead of a function pointer in priority_queue operations.
struct TimerEntryComparator
{
    bool operator()(const TimerEntry &a, const TimerEntry &b) const
    {
        return a.timepoint > b.timepoint;
    }
};
struct SleepAwaiter
{
    TimerChannel &channel;
    std::chrono::steady_clock::time_point due;

    SleepAwaiter(TimerChannel &channel, std::chrono::steady_clock::time_point due) noexcept : channel(channel), due(due)
    {
    }

    SleepAwaiter(const SleepAwaiter &) = delete;
    SleepAwaiter &operator=(const SleepAwaiter &) = delete;

    ~SleepAwaiter() = default;

    bool await_ready() const noexcept
    {
        return due <= std::chrono::steady_clock::now();
    }

    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle);

    void await_resume() noexcept
    {
    }
};
} // namespace detail

// TimerChannel waits for a dedicated timer to fire.
// In the event handler, the channel pops several due entries, each entry containing a coroutine.
class TimerChannel final : public Foundation::NBIO::Channel
{
    friend struct detail::SleepAwaiter;

  public:
    using Handle = Core::Timer::Handle;
    explicit TimerChannel(Foundation::Core::Timer &timer, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~TimerChannel() noexcept;

    Foundation::Core::Timer &timer() noexcept
    {
        return timer_;
    }
    const Foundation::Core::Timer &timer() const noexcept
    {
        return timer_;
    }

    void handle_completion();

    // Sleep 原语：挂起当前协程直到 due，定时器触发后由调度器恢复。
    // 返回的 awaiter 直接 co_await 即可。
    detail::SleepAwaiter sleep(std::chrono::steady_clock::time_point due) noexcept
    {
        return detail::SleepAwaiter{*this, due};
    }

    const std::size_t &last_expirations() const noexcept
    {
        return last_expirations_;
    }
    std::size_t &last_expirations() noexcept
    {
        return last_expirations_;
    }

  private:
    void park(Async::Coroutine coroutine_view, std::chrono::steady_clock::time_point due);
    bool remove(Async::Coroutine coroutine_view);

    Foundation::Core::Timer &timer_;
    Foundation::Core::PriorityQueue<detail::TimerEntry, detail::TimerEntryComparator> queue_;
    std::size_t last_expirations_{0};
};

template <typename PromiseType>
inline void detail::SleepAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle)
{
    auto coroutine = Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine), due);
}
} // namespace Foundation::NBIO
