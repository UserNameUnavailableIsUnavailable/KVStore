#include "EventNotifyChannel.hpp"

#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/core/Types.hpp>
#include <nbio/notification/EventNotifier.hpp>
#include <mutex>

namespace nbio::notification {
EventNotifyChannel::EventNotifyChannel(nbio::notification::EventNotifier& notifier,
                                       nbio::core::Multiplexer& multiplexer,
                                       nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<EventNotifyChannel>(nbio::core::ChannelType::kNotify,
                                                    static_cast<std::uintptr_t>(notifier.native_handle()), multiplexer,
                                                    scheduler),
    notifier_(notifier) {
    notifier.NonBlocking(true);

    // Nothing is armed and no read is prepared here: the channel has nothing to
    // watch until a waiter sleeps on the condition variable it serves, and that is
    // where it arms -- waiter_registered().
}

EventNotifyChannel::~EventNotifyChannel() noexcept { multiplexer_.DeleteChannel(this); }

EventNotifyChannel::Payload& EventNotifyChannel::Submit() { return payload_; }

void EventNotifyChannel::waiter_registered() {
    {
        std::lock_guard lock(mutex_);
        ++Parked_;
    }

    // One poll covers every waiter: what it reports is that the eventfd has
    // something, and the count waits there until this channel reads it out.
    Arm();
}

void EventNotifyChannel::Complete() {
    auto& payload = payload_;
    payload.release_poll();

    // Take the count out first: it is what makes the notification this poll
    // reported stop being reported, and the waiters below are who it was for.
    (void)notifier_.Wait();

    bool still_waiting = false;
    {
        std::lock_guard lock(mutex_);

        std::size_t woken = 0;
        for (auto& notifiee : notifiees_) {
            if (notifiee) [[likely]] {
                scheduler_.Submit(std::move(notifiee));
                ++woken;
            }
        }
        notifiees_.clear();
        Parked_ = Parked_ > woken ? Parked_ - woken : 0;

        // A waiter that is still asleep needs another poll: the notification that
        // wakes it may already have been counted in the eventfd.
        still_waiting = Parked_ > 0;
    }

    if (still_waiting) {
        Arm();
    } else {
        // Nobody is asleep on this condition variable, so there is nothing to
        // watch for.
        Disarm();
    }
}

void EventNotifyChannel::Park(nbio::async::Coroutine coroutine) {
    std::lock_guard lock(mutex_);
    notifiees_.push_back(std::move(coroutine));
    notifier_.Notify();
}

}  // namespace nbio::notification




