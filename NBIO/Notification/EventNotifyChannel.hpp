#pragma once

#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Notification/EventNotifier.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <cstddef>
#include <mutex>
#include <vector>

namespace NBIO::Notification {
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

class EventNotifyChannel;

class EventNotifyChannel final : public NBIO::Core::Channel<EventNotifyChannel> {
   public:
    using Payload = detail::PollPayload<EventNotifyChannel>;

    EventNotifyChannel(NBIO::Notification::EventNotifier& notifier, NBIO::Core::Multiplexer& multiplexer,
                       NBIO::Async::Scheduler& scheduler);
    ~EventNotifyChannel() noexcept;

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& submit();
    void complete();

    // A waiter is about to sleep on the condition variable this channel serves.
    // From here until it is resumed a notification has to be seen, so a read is
    // prepared and the channel armed. This runs on the thread the waiting
    // coroutine runs on -- this channel's own engine -- which is what makes arming
    // safe here; the handover below happens on whatever thread notifies, and only
    // touches the queue and the eventfd.
    void waiter_registered();

    template <typename It>
    void park(It begin, It end);

    void park(NBIO::Async::Coroutine notifiee);

    NBIO::Notification::EventNotifier& notifier() noexcept { return notifier_; }

    const NBIO::Notification::EventNotifier& notifier() const noexcept { return notifier_; }

   private:
    NBIO::Notification::EventNotifier& notifier_;
    std::mutex mutex_;
    std::vector<NBIO::Async::Coroutine> notifiees_;
    std::size_t parked_{0};
    Payload payload_{};
};

template <typename It>
inline void EventNotifyChannel::park(It begin, It end) {
    std::lock_guard lock(mutex_);
    notifiees_.insert(notifiees_.end(), begin, end);
    notifier_.notify();
}
}  // namespace NBIO::Notification




