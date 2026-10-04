#include "SystemSignalChannel.hpp"

#include <unistd.h>

#include <nbio/async/Scheduler.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <utility>

namespace nbio::signal {
SystemSignalChannel::SystemSignalChannel(nbio::signal::SystemSignal& signal,
                                         nbio::core::Multiplexer& multiplexer,
                                         nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<SystemSignalChannel>(nbio::core::ChannelType::kSystemSignal,
                                                     signal.native_handle(), multiplexer, scheduler),
      signal_(signal) {
    signal.NonBlocking(true);
    // Registered on the first Park(): nothing to watch until a coroutine waits.
}

SystemSignalChannel::~SystemSignalChannel() { multiplexer_.DeleteChannel(this); }

SystemSignalChannel::Payload& SystemSignalChannel::Submit() { return payload_; }

void SystemSignalChannel::Park(async::Coroutine coroutine) {
    waiters_.push_back(std::move(coroutine));
    Arm();
}

void SystemSignalChannel::Complete() {
    auto& payload = payload_;
    payload.release_poll();

    // Take the signals out first: that is what makes the signalfd stop reporting,
    // and the waiters below are who they were for.
    (void)signal_.drain();

    for (auto& waiter : waiters_) {
        // is_dead() first: it is what makes done() safe, since the frame may have
        // been reclaimed after the entry was queued.
        if (waiter) {
            scheduler_.Submit(std::move(waiter));
        }
    }

    waiters_.clear();

    // Every waiter is resumed above, so there is nothing left for a read to
    // report: a signalfd's read delivers the signals that are pending, and each
    // one is handed to all of them.
    Disarm();
}
}  // namespace nbio::signal




