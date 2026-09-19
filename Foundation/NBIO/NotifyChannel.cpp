#include "NotifyChannel.hpp"
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <Foundation/Core/Notifier.hpp>
#include <mutex>

namespace Foundation::NBIO
{
NotifyChannel::NotifyChannel(Foundation::Core::Notifier& notifier, Foundation::NBIO::Multiplexer& multiplexer, Foundation::Async::Scheduler& scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kNotify, static_cast<std::uintptr_t>(notifier.native_handle()), multiplexer, scheduler),
    notifier_(notifier)
{
    notifier.set_non_blocking(true);
    multiplexer.add_channel(this);

    // Nothing is armed and no read is prepared here: the channel has nothing to
    // watch until a waiter sleeps on the condition variable it serves, and that is
    // where it arms -- waiter_registered().
}

NotifyChannel::~NotifyChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

bool NotifyChannel::submit_job()
{
    if (submitted_)
    {
        return false; // the poll is already out there
    }
    {
        std::lock_guard lock(mutex_);
        if (parked_ == 0)
        {
            return false; // nobody is asleep, so there is nothing to watch for
        }
    }

    submitted_ = true;
    return true;
}

void NotifyChannel::advance_job(std::ptrdiff_t) noexcept
{
    // A poll's answer says only that the eventfd has something; the count is read
    // out in handle_completion().
}

void NotifyChannel::complete_job() noexcept
{
    submitted_ = false;
}

void NotifyChannel::waiter_registered()
{
    {
        std::lock_guard lock(mutex_);
        ++parked_;
    }

    // One poll covers every waiter: what it reports is that the eventfd has
    // something, and the count waits there until this channel reads it out.
    arm();
}

void NotifyChannel::handle_completion()
{
    // Take the count out first: it is what makes the notification this poll
    // reported stop being reported, and the waiters below are who it was for.
    (void)notifier_.wait();

    bool still_waiting = false;
    {
        std::lock_guard lock(mutex_);

        std::size_t woken = 0;
        for (auto &notifiee : notifiees_)
        {
            if (notifiee) [[likely]]
            {
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

    if (still_waiting)
    {
        arm();
    }
    else
    {
        // Nobody is asleep on this condition variable, so there is nothing to
        // watch for.
        disarm();
    }
}

void NotifyChannel::park(Foundation::Async::Coroutine coroutine)
{
    std::lock_guard lock(mutex_);
    notifiees_.push_back(std::move(coroutine));
    notifier_.notify();
}

} // namespace Foundation::NBIO
