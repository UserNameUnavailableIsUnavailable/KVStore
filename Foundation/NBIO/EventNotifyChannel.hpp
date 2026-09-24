#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/EventNotifier.hpp>
#include <Foundation/NBIO/Payload.hpp>

#include <cstddef>
#include <mutex>
#include <vector>

namespace Foundation::NBIO
{
class EventNotifyChannel;

class EventNotifyChannel final : public Foundation::NBIO::Channel
{
public:
    EventNotifyChannel(Foundation::Core::EventNotifier& notifier, Foundation::NBIO::Multiplexer& multiplexer, Foundation::Async::Scheduler& scheduler);
    ~EventNotifyChannel() noexcept;

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload &submit();
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

    void park(Foundation::Async::Coroutine notifiee);

    Foundation::Core::EventNotifier& notifier() noexcept
    {
        return notifier_;
    }

    const Foundation::Core::EventNotifier& notifier() const noexcept
    {
        return notifier_;
    }

private:
    Foundation::Core::EventNotifier& notifier_;
    std::mutex mutex_;
    std::vector<Foundation::Async::Coroutine> notifiees_;
    std::size_t parked_{0};
    Payload payload_{NotifyPayload{}};
};

template <typename It>
inline void EventNotifyChannel::park(It begin, It end)
{
    std::lock_guard lock(mutex_);
    notifiees_.insert(notifiees_.end(), begin, end);
    notifier_.notify();
}
} // namespace Foundation::NBIO
