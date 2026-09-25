#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

namespace Foundation::NBIO
{
// The thread's signal wait, as a handle: `co_await SystemSignalService{}.wait()`.
//
// What it waits for is SIGINT or SIGTERM, whichever comes first, which is the
// shutdown idiom: `co_await when_any(AcceptLoop(), SystemSignalService{}.wait())`.
//
// Like the timer, the wait belongs to the runtime rather than to the handle, so the
// engine is resolved where the wait is queued and a handle owns nothing.
//
// One thing is deliberately *not* done here: constructing a handle does not touch the
// process's signal disposition. Installing the handlers is what the first wait does,
// because a runtime that intercepts SIGINT the moment somebody declares a service
// would be a nasty thing to find in a program that handles its own signals.
class SystemSignalService final
{
  public:
    SystemSignalService() noexcept = default;

    // Suspends until a signal is delivered, and installs the handlers if this is the
    // first wait on this thread.
    Foundation::NBIO::Task<void> wait() const
    {
        return WaitForSignal();
    }

  private:
    // The wait body, as a plain coroutine, and inline because it lives in a header.
    static Foundation::NBIO::Task<void> WaitForSignal()
    {
        co_await Engine::signal_channel().wait();
    }
};
} // namespace Foundation::NBIO
