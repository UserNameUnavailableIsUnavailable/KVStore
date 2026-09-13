#include "NotifyChannel.hpp"
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/NBIO/Types.hpp>
#include <Foundation/Core/Notifier.hpp>
#include <mutex>

namespace Foundation::NBIO
{
NotifyChannel::NotifyChannel(Foundation::Core::Notifier& notifier, Foundation::NBIO::Multiplexer& multiplexer, Foundation::Async::Scheduler& scheduler) :
    Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kNotify, notifier.native_handle(), multiplexer, scheduler),
    notifier_(notifier)
{
    notifier.set_non_blocking(true);
    multiplexer.add_channel(this);
    arm();
}

NotifyChannel::~NotifyChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void NotifyChannel::handle_event()
{
    // handle_event runs in a single thread
    if (handler_) [[likely]]
    {
        handler_(this);
    }
    {
        // other threads may operate on notifiee list
        // this lock only protects notifiees_
        std::lock_guard lock(mutex_);
        for (auto &notifiee : notifiees_)
        {
            if (notifiee) [[likely]]
            {
                scheduler_.submit(std::move(notifiee));
            }
        }
        notifiees_.clear();
    }
    // we should unconditionally arm the notify channel
    // notify channel serves multiple waiters
    // any thread may park a new coroutine and issue a notification at any time
    arm();
}

void NotifyChannel::park(Foundation::Async::Coroutine coroutine)
{
    std::lock_guard lock(mutex_);
    notifiees_.push_back(std::move(coroutine));
    notifier_.notify();
}

} // namespace Foundation::NBIO
