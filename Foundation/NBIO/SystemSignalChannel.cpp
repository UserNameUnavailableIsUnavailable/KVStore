#include "SystemSignalChannel.hpp"

#include <unistd.h>

#include <utility>

#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::NBIO
{
SystemSignalChannel::SystemSignalChannel(Foundation::Core::SystemSignal &signal, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kSystemSignal, signal.native_handle(), multiplexer, scheduler), signal_(signal)
{
    signal.non_blocking(true);
    // Registered on the first park(): nothing to watch until a coroutine waits.
}

SystemSignalChannel::~SystemSignalChannel()
{
    multiplexer_.delete_channel(this);
}

Payload &SystemSignalChannel::submit()
{
    return payload_;
}

void SystemSignalChannel::park(Async::Coroutine coroutine)
{
    waiters_.push_back(std::move(coroutine));
    arm();
}

void SystemSignalChannel::complete()
{
    auto &payload = std::get<SystemSignalPayload>(payload_);
    payload.release_poll();

    // Take the signals out first: that is what makes the signalfd stop reporting,
    // and the waiters below are who they were for.
    (void)signal_.drain();

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

    // Every waiter is resumed above, so there is nothing left for a read to
    // report: a signalfd's read delivers the signals that are pending, and each
    // one is handed to all of them.
    disarm();
}
} // namespace Foundation::NBIO
