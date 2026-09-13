#pragma once

#include <Foundation/Async/Scheduler.hpp>

#include <Foundation/Core/Notifier.hpp>
#include <Foundation/Core/Signal.hpp>
#include <Foundation/Core/Timer.hpp>

#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <functional>
#include <memory>

#include "Multiplexer.hpp"
#include "NotifyChannel.hpp"
#include "SignalChannel.hpp"
#include "TimerChannel.hpp"

namespace Foundation::NBIO
{
// The NBIO runtime for one thread, and the runtime tag that Foundation::Async
// tasks are parameterized on.
//
// It owns everything the backend needs: the multiplexer, the scheduler built on
// its idle hook, and the standing channels plus the resources they bind to.
// Static accessors hand those out for whichever engine is installed on the
// calling thread. Foundation::Async never sees any of this -- it asks the tag
// for a scheduler and nothing more.
//
// Member order matters twice. The multiplexer is declared first so the
// scheduler's idle hook can capture it, and the channels come last so they are
// destroyed first: every channel unregisters itself through
// multiplexer_.delete_channel() in its destructor, which requires a live
// multiplexer. Each channel references its resource, so each resource is
// declared before (and destroyed after) the channel bound to it.
class Engine
{
  public:
    Engine(const Engine &) = delete;
    Engine &operator=(const Engine &) = delete;
    Engine(Engine &&) = delete;
    Engine &operator=(Engine &&) = delete;

    ~Engine() noexcept;

    // Installs a backend on the current thread. Throws if one is already
    // installed there.
    static void initialize(std::unique_ptr<Multiplexer> multiplexer);

    // Whether a backend has been installed on this thread.
    static bool is_initialized()
    {
      return static_cast<bool>(engine_);
    }

    // The engine current on this thread. Throws when installed() is false.
    static Engine &instance();

    // ---- what Foundation::Async asks of a runtime tag ----
    static Foundation::Async::Scheduler &scheduler();
    static Multiplexer &multiplexer();

    // The channel carrying application-generated events. ConditionVariable
    // binds to it, so an application event travels the same path as a kernel
    // event: hand the waiter over, let the multiplexer dispatch it.
    static NotifyChannel &notify_channel();

    // The standing channels owned by the engine.
    static TimerChannel &timer_channel();

    // Lazily created: constructing the signal channel intercepts SIGINT and
    // SIGTERM, which must only happen if the application asks for it.
    static SignalChannel &signal_channel();

  private:
    explicit Engine(std::unique_ptr<Multiplexer> multiplexer);

    static std::function<void(bool)> make_idle_hook(Multiplexer &multiplexer);

    std::unique_ptr<Multiplexer> multiplexer_;
    Foundation::Async::Scheduler scheduler_;
    Foundation::Core::Timer timer_;
    TimerChannel timer_channel_;
    Foundation::Core::Notifier notifier_;
    NotifyChannel notify_channel_;
    std::unique_ptr<Foundation::Core::Signal> signal_;
    std::unique_ptr<SignalChannel> signal_channel_;

    static thread_local std::unique_ptr<Engine> engine_;
};

// Creates the platform default backend (epoll on Linux).
std::unique_ptr<Multiplexer> make_default_multiplexer();
} // namespace Foundation::NBIO
