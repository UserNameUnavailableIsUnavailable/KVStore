#pragma once

#include <Foundation/Core/PriorityQueue.hpp>
#include <Foundation/Core/Timer.hpp>
#include <chrono>
#include <coroutine>
#include <type_traits>

#include "Channel.hpp"
#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
class TimerChannel;
namespace detail
{

struct TimerEntry
{
    std::coroutine_handle<> handle{};
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
    std::coroutine_handle<> handle_{};

    SleepAwaiter(TimerChannel &channel, std::chrono::steady_clock::time_point due) noexcept : channel(channel), due(due)
    {
    }

    SleepAwaiter(const SleepAwaiter &) = delete;
    SleepAwaiter &operator=(const SleepAwaiter &) = delete;

    // cancellation hook (Tokio-style): if the coroutine frame is destroyed
    // while still parked in the timer queue, remove the entry so the timer
    // never Submits a dangling handle. Defined out-of-line (TimerChannel is
    // incomplete here). Harmless on the normal path (await_resume clears
    // handle_) and if the entry already fired (remove finds nothing).
    ~SleepAwaiter();

    // 已过期则不挂起（fast-path）：直接恢复，省一次入队/出队。
    bool await_ready() const noexcept
    {
        return due <= std::chrono::steady_clock::now();
    }

    // 模板化：coroutine_handle 没有派生→基类转换，编译器传入的是
    // coroutine_handle<promise_type>，基类 Promise 经引用向上转换获得。
    // 注意：定义在 TimerChannel 类之后（此处 TimerChannel 尚不完整）。
    template <typename PromiseType> void await_suspend(std::coroutine_handle<PromiseType> handle);

    void await_resume() noexcept
    {
        // 能走到这里说明是“定时器触发”路径（取消路径不恢复协程），
        // 清掉 handle_ 使析构成为 no-op（entry 已被 OnEvent 出队）。
        handle_ = {};
    }
};
} // namespace detail

// TimerChannel waits for a dedicated timer to fire.
// In the event handler, the channel pops several due entries, each entry containing a coroutine.
class TimerChannel final : public Channel
{
    friend struct detail::SleepAwaiter;

  public:
    explicit TimerChannel(Foundation::Core::Timer &timer, Multiplexer &multiplexer, Scheduler &scheduler);
    virtual ~TimerChannel() noexcept override;

    Foundation::Core::Timer &timer() noexcept
    {
        return timer_;
    }
    const Foundation::Core::Timer &timer() const noexcept
    {
        return timer_;
    }

    virtual void on_event() override;

    // Sleep 原语：挂起当前协程直到 due，定时器触发后由调度器恢复。
    // 返回的 awaiter 直接 co_await 即可（TimerService 的 SleepFor/SleepUntil
    // 就是对它的封装）。
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
    void insert(std::coroutine_handle<> handle, std::chrono::steady_clock::time_point due);
    bool remove(std::coroutine_handle<> handle);

    Foundation::Core::Timer &timer_;
    Foundation::Core::PriorityQueue<detail::TimerEntry, detail::TimerEntryComparator> queue_;
    std::size_t last_expirations_{0};
};

template <typename PromiseType>
inline void detail::SleepAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle)
{
    static_assert(std::is_base_of_v<Promise, PromiseType>, "SleepAwaiter requires a promise derived from Promise");
    handle_ = handle;
    channel.insert(handle, due);
}

inline detail::SleepAwaiter::~SleepAwaiter()
{
    // cancellation: frame destroyed while still parked (await_resume never
    // ran). Pull the entry out so on_event never Submits a dangling handle.
    if (handle_)
    {
        channel.remove(handle_);
    }
}
} // namespace Foundation::Async
