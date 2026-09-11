#include "SignalChannel.hpp"

#include <unistd.h>

#include <utility>

#include "Multiplexer.hpp"
#include "Scheduler.hpp"

namespace Foundation::Async
{
SignalChannel::SignalChannel(Foundation::Core::Signal &signal, Multiplexer &multiplexer, Scheduler &scheduler)
    : Channel(ChannelType::kSignal, signal.native_handle(), multiplexer, scheduler), signal_(signal)
{
    signal.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

SignalChannel::~SignalChannel()
{
    multiplexer_.delete_channel(this);
}

void SignalChannel::insert(std::coroutine_handle<> handle)
{
    waiters_.push_back(handle);
    if (!armed())
    {
        arm();
    }
}

bool SignalChannel::remove(std::coroutine_handle<> handle)
{
    waiters_.remove(handle);
    if (waiters_.empty() && armed())
    {
        disarm();
    }
    return true;
}

void SignalChannel::on_event()
{
    if (count_ == 0)
    {
        while (::read(native_handle(), &count_, sizeof(count_)) == static_cast<ssize_t>(sizeof(count_)))
        {
        }
    }
    count_ = 0;

    armed_ = false;
    auto waiters = std::exchange(waiters_, {});
    for (auto waiter : waiters)
    {
        if (waiter && !waiter.done())
        {
            scheduler_.submit(waiter);
        }
    }
}
} // namespace Foundation::Async
