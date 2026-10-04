#pragma once

#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Core/SystemSignal.hpp>
#include <NBIO/Core/SystemTimer.hpp>
#include <NBIO/Core/EpollMultiplexer.hpp>
#include <functional>
#include <memory>

#include <NBIO/Notification/EventNotifyChannel.hpp>
#include <NBIO/Notification/EventNotifier.hpp>
#include "Multiplexer.hpp"
#include <NBIO/Signal/SystemSignalChannel.hpp>
#include <NBIO/Time/SystemTimerChannel.hpp>

namespace NBIO::Core {
// The NBIO runtime for one thread, and the runtime tag that NBIO::Async
// tasks are parameterized on.
//
// It owns everything the backend needs: the multiplexer, the scheduler built on
// its idle hook, and the standing channels plus the resources they bind to.
// Static accessors hand those out for whichever engine is installed on the
// calling thread. NBIO::Async never sees any of this -- it asks the tag
// for a scheduler and nothing more.
//
// Member order matters twice. The multiplexer is declared first so the
// scheduler's idle hook can capture it, and the channels come last so they are
// destroyed first: every channel unregisters itself through
// multiplexer_.delete_channel() in its destructor, which requires a live
// multiplexer. Each channel references its resource, so each resource is
// declared before (and destroyed after) the channel bound to it.
class Engine {
   public:
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
    Engine(Engine&&) = delete;
    Engine& operator=(Engine&&) = delete;

    ~Engine() noexcept;

    // Installs a backend on the current thread. Throws if one is already
    // installed there.
    static void initialize(std::unique_ptr<Multiplexer> multiplexer);

    // Whether a backend has been installed on this thread.
    static bool is_initialized() { return static_cast<bool>(engine_); }

    // The engine current on this thread. Throws when installed() is false.
    static Engine& instance();

    // ---- what NBIO::Async asks of a runtime tag ----
    static NBIO::Async::Scheduler& scheduler();
    static Multiplexer& multiplexer();

    // The channel carrying application-generated events. ConditionVariable
    // binds to it, so an application event travels the same path as a kernel
    // event: hand the waiter over, let the multiplexer dispatch it.
    static Notification::EventNotifyChannel& notify_channel();

    // The standing channels owned by the engine.
    static Time::SystemTimerChannel& timer_channel();

    // Lazily created: constructing the signal channel intercepts SIGINT and
    // SIGTERM, which must only happen if the application asks for it.
    static Signal::SystemSignalChannel& signal_channel();

   private:
    explicit Engine(std::unique_ptr<Multiplexer> multiplexer);

    static std::function<void(bool)> make_idle_hook(Multiplexer& multiplexer);

    std::unique_ptr<Multiplexer> multiplexer_;
    NBIO::Async::Scheduler scheduler_;
    NBIO::Core::SystemTimer timer_;
    Time::SystemTimerChannel timer_channel_;
    NBIO::Notification::EventNotifier notifier_;
    Notification::EventNotifyChannel notify_channel_;
    std::unique_ptr<NBIO::Core::SystemSignal> signal_;
    std::unique_ptr<Signal::SystemSignalChannel> signal_channel_;

    static thread_local std::unique_ptr<Engine> engine_;
};

// Creates the platform default backend
std::unique_ptr<Multiplexer> make_default_multiplexer();
}  // namespace NBIO::Core


