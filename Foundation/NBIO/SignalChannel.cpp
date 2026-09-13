#include "SignalChannel.hpp"

#include <unistd.h>

#include <utility>

#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::NBIO
{
SignalChannel::SignalChannel(Foundation::Core::Signal &signal, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kSignal, signal.native_handle(), multiplexer, scheduler), signal_(signal)
{
    signal.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

SignalChannel::~SignalChannel()
{
    multiplexer_.delete_channel(this);
}

void SignalChannel::park(Async::Coroutine coroutine)
{
    waiters_.push_back(std::move(coroutine));
    arm();
}

void SignalChannel::handle_event()
{
    if (handler_) [[likely]]
    {
        handler_(this);
    }

    for (auto &waiter : waiters_)
    {
        // is_dead() first: it is what makes done() safe, since the frame may have
        // been reclaimed after the entry was queued.
        if (waiter)
        {
            scheduler_.submit(std::move(waiter));
        }
    }

    waiters_.clear();
}
} // namespace Foundation::NBIO
