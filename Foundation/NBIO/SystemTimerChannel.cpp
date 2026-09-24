#include "SystemTimerChannel.hpp"

namespace Foundation::NBIO
{
SystemTimerChannel::SystemTimerChannel(Foundation::Core::SystemTimer &timer, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kSystemTimer, timer.native_handle(), multiplexer, scheduler), timer_(timer), queue_()
{
    timer_.non_blocking(true);
    // Registered on the first arm(): nothing to watch until a sleep queues.
}

SystemTimerChannel::~SystemTimerChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void SystemTimerChannel::park(Async::Coroutine coroutine_view, std::chrono::steady_clock::time_point due)
{
    queue_.emplace(coroutine_view, due);
    timer_.fire_at(queue_.top().timepoint);
}

Payload &SystemTimerChannel::submit()
{
    return payload_;
}

bool SystemTimerChannel::remove(Async::Coroutine coroutine_view)
{
    const bool removed = queue_.remove_if([&](const detail::SystemTimerEntry &entry) {
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

void SystemTimerChannel::complete()
{
    auto &payload = std::get<SystemTimerPayload>(payload_);
    payload.release_poll();

    // Take the expirations out first: that is what makes the timerfd stop
    // reporting, and the entries below are the ones it is reporting for.
    (void)timer_.wait();

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
