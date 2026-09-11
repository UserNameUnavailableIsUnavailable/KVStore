#include "NotifyChannel.hpp"
#include <Foundation/Async/Channel.hpp>
#include <Foundation/Async/Multiplexer.hpp>
#include <Foundation/Async/Types.hpp>
#include <Foundation/Core/Notifier.hpp>
#include <coroutine>
#include <mutex>

namespace Foundation::Async
{
NotifyChannel::NotifyChannel(Foundation::Core::Notifier& notifier, Multiplexer& multiplexer, Scheduler& scheduler) :
    Channel(ChannelType::kNotify, notifier.native_handle(), multiplexer, scheduler),
    notifier_(notifier)
{
    notifier.set_non_blocking(true);
    multiplexer.add_channel(this);
}

NotifyChannel::~NotifyChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

void NotifyChannel::on_event()
{
    if (handler_)
    {
        handler_(this);
    }
    std::lock_guard lock(mutex_);
    for (auto& notifiee : notifiees_)
    {
        if (notifiee)
        {
            scheduler_.submit(notifiee);
        }
    }
    notifiees_.clear();

    arm(); // re-arm the channel
}

void NotifyChannel::submit(std::coroutine_handle<> h)
{
    std::lock_guard lock(mutex_);
    notifiees_.emplace_back(h);
    notifier_.notify();
}
} // namespace Foundation::Async