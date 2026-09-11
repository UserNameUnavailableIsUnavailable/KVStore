#include "TimerChannel.hpp"

namespace Foundation::Async
{
TimerChannel::TimerChannel(Timer &timer, Multiplexer &multiplexer, Scheduler &scheduler)
    : Channel(ChannelType::kTimer, timer.native_handle(), multiplexer, scheduler), timer_(timer), queue_()
{
    timer_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

TimerChannel::~TimerChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void TimerChannel::insert(std::coroutine_handle<> handle, std::chrono::steady_clock::time_point due)
{
    queue_.emplace(handle, due);
    timer_.fire_at(queue_.top().timepoint);
    // 武装 channel，让多路复用器把 timerfd 的可读事件纳入监听，
    // 否则定时器到期也永远不会被上报（协程将永远挂起）。
    if (!armed())
    {
        arm();
    }
}

bool TimerChannel::remove(std::coroutine_handle<> handle)
{
    const bool removed = queue_.remove_if([handle](const detail::TimerEntry &entry) { return entry.handle == handle; });
    if (removed)
    {
        if (queue_.is_empty())
        {
            timer_.cancel();
            if (armed())
            {
                disarm();
            }
        }
        else
        {
            timer_.fire_at(queue_.top().timepoint);
        }
    }
    return removed;
}

void TimerChannel::on_event()
{
    // A fired timer is one-shot: the epoll backend disarms us before dispatch,
    // the io_uring backend consumes the read SQE. Either way the registration
    // is gone now, so reflect that here (backend-agnostic) and re-arm below if
    // there is still work -- rather than relying on the multiplexer to disarm.
    armed_ = false;

    if (handler_)
    {
        handler_(this);
    }
    while (!queue_.is_empty())
    {
        const auto &top = queue_.top();
        if (top.timepoint > std::chrono::steady_clock::now())
        {
            break;
        }
        auto h = top.handle;
        queue_.pop();
        scheduler_.submit(h);
    }
    if (!queue_.is_empty())
    {
        auto tp = queue_.top().timepoint;
        auto dur = tp - std::chrono::steady_clock::now();
        if (dur < std::chrono::milliseconds(0))
        {
            dur = std::chrono::milliseconds(0);
        }
        timer_.fire_after(dur);
        // armed_ was reset above, so re-arm to receive the next expiration.
        arm();
    }
}
} // namespace Foundation::Async
