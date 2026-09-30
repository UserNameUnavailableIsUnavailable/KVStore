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

void SystemTimerChannel::park(Async::Coroutine coroutine, std::chrono::steady_clock::time_point due)
{
    if (due <= std::chrono::steady_clock::now())
    {
        const bool was_empty = immediate_queue_.empty();
        immediate_queue_.push(std::move(coroutine));
        if (was_empty)
        {
            timer_.fire_after(std::chrono::nanoseconds{1});
            arm();
        }
        return;
    }

    queue_.emplace(coroutine, due);
    timer_.fire_at(queue_.top().timepoint);
}

Payload &SystemTimerChannel::submit()
{
    return payload_;
}

bool SystemTimerChannel::remove(Async::Coroutine coroutine_view)
{
    bool removed_immediate = false;
    std::queue<Async::Coroutine> filtered_immediate;
    while (!immediate_queue_.empty())
    {
        auto current = std::move(immediate_queue_.front());
        immediate_queue_.pop();
        if (current.handle == coroutine_view.handle)
        {
            removed_immediate = true;
            continue;
        }
        filtered_immediate.push(std::move(current));
    }
    immediate_queue_ = std::move(filtered_immediate);

    const bool removed_delayed = queue_.remove_if([&](const detail::SystemTimerEntry &entry) {
        return entry.coroutine_view.handle == coroutine_view.handle;
    });
    const bool removed = removed_immediate || removed_delayed;
    if (removed)
    {
        if (!immediate_queue_.empty())
        {
            if (!armed())
            {
                arm();
            }
            timer_.fire_after(std::chrono::nanoseconds{1});
        }
        else if (queue_.is_empty())
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
    auto result = timer_.wait();
    if (!result)
    {
        throw std::runtime_error(std::format("failed to wait timer: {}", result.error().message()));
    }

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

    while (!immediate_queue_.empty())
    {
        scheduler_.submit(std::move(immediate_queue_.front()));
        immediate_queue_.pop();
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
