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

bool SignalChannel::submit_job()
{
    if (submitted_)
    {
        return false; // the poll is already out there
    }
    if (waiters_.empty())
    {
        return false; // nothing to wait for
    }

    submitted_ = true;
    return true;
}

void SignalChannel::advance_job(std::ptrdiff_t) noexcept
{
    // A poll's answer says only that the signalfd became readable; the signals are
    // drained in handle_completion().
}

void SignalChannel::complete_job() noexcept
{
    submitted_ = false;
}

void SignalChannel::handle_completion()
{
    // Take the signals out first: that is what makes the signalfd stop reporting,
    // and the waiters below are who they were for.
    signal_.drain();

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
