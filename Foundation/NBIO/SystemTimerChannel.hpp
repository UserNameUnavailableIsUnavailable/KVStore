#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/PriorityQueue.hpp>
#include <Foundation/Core/SystemTimer.hpp>
#include <Foundation/NBIO/Payload.hpp>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <Foundation/Async/Coroutine.hpp>

namespace Foundation::NBIO
{
class SystemTimerChannel;
namespace detail
{

struct SystemTimerEntry
{
    // std::coroutine_handle<> coroutine_view{};
    Async::Coroutine coroutine_view;
    std::chrono::steady_clock::time_point timepoint{};
};
// Stateless comparator functor: operator() can be inlined, avoiding the
// indirect-call overhead of a function pointer in priority_queue operations.
struct SystemTimerEntryComparator
{
    bool operator()(const SystemTimerEntry &a, const SystemTimerEntry &b) const
    {
        return a.timepoint > b.timepoint;
    }
};
struct SleepAwaiter
{
    SystemTimerChannel &channel;
    std::chrono::steady_clock::time_point due;

    SleepAwaiter(SystemTimerChannel &channel, std::chrono::steady_clock::time_point due) noexcept : channel(channel), due(due)
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

// SystemTimerChannel waits for a dedicated timer to fire.
// In the event handler, the channel pops several due entries, each entry containing a coroutine.
class SystemTimerChannel final : public Foundation::NBIO::Channel
{
    friend struct detail::SleepAwaiter;

  public:
    using Handle = Core::SystemTimer::Handle;
    explicit SystemTimerChannel(Foundation::Core::SystemTimer &timer, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~SystemTimerChannel() noexcept;

    Foundation::Core::SystemTimer &timer() noexcept
    {
        return timer_;
    }
    const Foundation::Core::SystemTimer &timer() const noexcept
    {
        return timer_;
    }

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload &submit();
    void complete();

    // Sleep 原语：挂起当前协程直到 due，定时器触发后由调度器恢复。
    // 返回的 awaiter 直接 co_await 即可。
    detail::SleepAwaiter sleep(std::chrono::steady_clock::time_point due) noexcept
    {
        return detail::SleepAwaiter{*this, due};
    }

  private:
    void park(Async::Coroutine coroutine_view, std::chrono::steady_clock::time_point due);
    bool remove(Async::Coroutine coroutine_view);

    Foundation::Core::SystemTimer &timer_;
    Foundation::Core::PriorityQueue<detail::SystemTimerEntry, detail::SystemTimerEntryComparator> queue_;
    Payload payload_{SystemTimerPayload{}};
};

template <typename PromiseType>
inline void detail::SleepAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle)
{
    auto coroutine = Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine), due);
}
} // namespace Foundation::NBIO
