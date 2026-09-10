#pragma once

#include <Foundation/Signal.hpp>

#include "SignalChannel.hpp"

namespace Foundation::Async
{
class SignalService
{
  public:
    SignalService(Multiplexer &multiplexer, Scheduler &scheduler) : signal_(), channel_(signal_, multiplexer, scheduler)
    {
    }

    SignalService(const SignalService &) = delete;
    SignalService &operator=(const SignalService &) = delete;
    SignalService(SignalService &&) = delete;
    SignalService &operator=(SignalService &&) = delete;

    SignalChannel::Awaiter wait() noexcept
    {
        return channel_.wait();
    }

  private:
    Signal signal_;
    SignalChannel channel_;
};
} // namespace Foundation::Async
