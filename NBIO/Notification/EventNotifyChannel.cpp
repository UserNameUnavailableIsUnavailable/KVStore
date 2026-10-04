#include "EventNotifyChannel.hpp"

#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Core/Types.hpp>
#include <NBIO/Notification/EventNotifier.hpp>
#include <mutex>

namespace NBIO::Notification {
EventNotifyChannel::EventNotifyChannel(NBIO::Notification::EventNotifier& notifier,
                                       NBIO::Core::Multiplexer& multiplexer,
                                       NBIO::Async::Scheduler& scheduler)
    : NBIO::Core::Channel<EventNotifyChannel>(NBIO::Core::ChannelType::kNotify,
                                                    static_cast<std::uintptr_t>(notifier.native_handle()), multiplexer,
                                                    scheduler),
    notifier_(notifier) {
    notifier.non_blocking(true);

    // Nothing is armed and no read is prepared here: the channel has nothing to
    // watch until a waiter sleeps on the condition variable it serves, and that is
    // where it arms -- waiter_registered().
}

EventNotifyChannel::~EventNotifyChannel() noexcept { multiplexer_.delete_channel(this); }

EventNotifyChannel::Payload& EventNotifyChannel::submit() { return payload_; }

void EventNotifyChannel::waiter_registered() {
    {
        std::lock_guard lock(mutex_);
        ++parked_;
    }

    // One poll covers every waiter: what it reports is that the eventfd has
    // something, and the count waits there until this channel reads it out.
    arm();
}

void EventNotifyChannel::complete() {
    auto& payload = payload_;
    payload.release_poll();

    // Take the count out first: it is what makes the notification this poll
    // reported stop being reported, and the waiters below are who it was for.
    (void)notifier_.wait();

    bool still_waiting = false;
    {
        std::lock_guard lock(mutex_);

        std::size_t woken = 0;
        for (auto& notifiee : notifiees_) {
            if (notifiee) [[likely]] {
                scheduler_.submit(std::move(notifiee));
                ++woken;
            }
        }
        notifiees_.clear();
        parked_ = parked_ > woken ? parked_ - woken : 0;

        // A waiter that is still asleep needs another poll: the notification that
        // wakes it may already have been counted in the eventfd.
        still_waiting = parked_ > 0;
    }

    if (still_waiting) {
        arm();
    } else {
        // Nobody is asleep on this condition variable, so there is nothing to
        // watch for.
        disarm();
    }
}

void EventNotifyChannel::park(NBIO::Async::Coroutine coroutine) {
    std::lock_guard lock(mutex_);
    notifiees_.push_back(std::move(coroutine));
    notifier_.notify();
}

}  // namespace NBIO::Notification




