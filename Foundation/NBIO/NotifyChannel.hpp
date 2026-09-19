#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Notifier.hpp>

#include <cstddef>
#include <mutex>
#include <vector>

namespace Foundation::NBIO
{
class NotifyChannel;

class NotifyChannel final : public Foundation::NBIO::Channel
{
public:
    NotifyChannel(Foundation::Core::Notifier& notifier, Foundation::NBIO::Multiplexer& multiplexer, Foundation::Async::Scheduler& scheduler);
    ~NotifyChannel() noexcept;

    // The one-job protocol (see Channel.hpp), as the channels that carry a single
    // wait keep it. What the backend is asked for here is a poll, not a read: a
    // notification is counted in the eventfd rather than read into a buffer, so the
    // wait itself is what is prepared and a job has nothing to hold.
    bool submit_job();
    void advance_job(std::ptrdiff_t result) noexcept;
    void complete_job() noexcept;

    // A waiter is about to sleep on the condition variable this channel serves.
    // From here until it is resumed a notification has to be seen, so a read is
    // prepared and the channel armed. This runs on the thread the waiting
    // coroutine runs on -- this channel's own engine -- which is what makes arming
    // safe here; the handover below happens on whatever thread notifies, and only
    // touches the queue and the eventfd.
    void waiter_registered();

    void handle_completion();

    template <typename It>
    void park(It begin, It end);

    void park(Foundation::Async::Coroutine notifiee);

    Foundation::Core::Notifier& notifier() noexcept
    {
        return notifier_;
    }

    const Foundation::Core::Notifier& notifier() const noexcept
    {
        return notifier_;
    }

private:
    Foundation::Core::Notifier& notifier_;
    std::mutex mutex_;
    std::vector<Foundation::Async::Coroutine> notifiees_;
    // Waiters registered and not yet resumed. The ones that are asleep are on the
    // condition variable rather than here, so this count is the only way the
    // channel can tell whether anything still needs to be watched.
    std::size_t parked_{0};
    // Whether the poll that reports a notification is out there. Only the engine
    // thread touches it: the backend asks while submitting, and the channel clears
    // it when the poll completes.
    bool submitted_{false};
};

template <typename It>
inline void NotifyChannel::park(It begin, It end)
{
    std::lock_guard lock(mutex_);
    notifiees_.insert(notifiees_.end(), begin, end);
    notifier_.notify();
}
} // namespace Foundation::NBIO
