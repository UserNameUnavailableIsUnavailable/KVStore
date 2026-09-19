#include "TimerChannel.hpp"

namespace Foundation::NBIO
{
TimerChannel::TimerChannel(Foundation::Core::Timer &timer, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kTimer, timer.native_handle(), multiplexer, scheduler), timer_(timer), queue_()
{
    timer_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

TimerChannel::~TimerChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void TimerChannel::park(Async::Coroutine coroutine_view, std::chrono::steady_clock::time_point due)
{
    queue_.emplace(coroutine_view, due);
    timer_.fire_at(queue_.top().timepoint);
}

bool TimerChannel::submit_job()
{
    if (submitted_)
    {
        return false; // the poll is already out there
    }
    if (queue_.is_empty())
    {
        return false; // nothing to wait for
    }

    submitted_ = true;
    return true;
}

void TimerChannel::advance_job(std::ptrdiff_t) noexcept
{
    // A poll's answer says only that the timerfd became readable; the expirations
    // are drained in handle_completion().
}

void TimerChannel::complete_job() noexcept
{
    submitted_ = false;
}

bool TimerChannel::remove(Async::Coroutine coroutine_view)
{
    const bool removed = queue_.remove_if([&](const detail::TimerEntry &entry) {
        return entry.coroutine_view.handle == coroutine_view.handle;
    });
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

void TimerChannel::handle_completion()
{
    // Take the expirations out first: that is what makes the timerfd stop
    // reporting, and the entries below are the ones it is reporting for.
    timer_.wait();

    while (!queue_.is_empty()) [[likely]]
    {
        const auto &top = queue_.top();
        if (top.timepoint > std::chrono::steady_clock::now())
        {
            break;
        }
        auto& cv = top.coroutine_view;
        scheduler_.submit(std::move(cv));
        queue_.pop();
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
        arm();
    }
    else
    {
        // Nothing left to wait for.
        disarm();
    }
}
} // namespace Foundation::NBIO
