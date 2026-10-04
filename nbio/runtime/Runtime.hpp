#pragma once

#include <nbio/async/Scheduler.hpp>
#include <nbio/signal/SystemSignal.hpp>
#include <nbio/time/SystemTimer.hpp>
#include <nbio/core/EpollMultiplexer.hpp>
#include <functional>
#include <memory>

#include <nbio/notification/EventNotifyChannel.hpp>
#include <nbio/notification/EventNotifier.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/signal/SystemSignalChannel.hpp>
#include <nbio/time/SystemTimerChannel.hpp>

namespace nbio {
// The nbio runtime for one thread, and the runtime tag that nbio::async
// tasks are parameterized on.
//
// It owns everything the backend needs: the multiplexer, the scheduler built on
// its idle hook, and the standing channels plus the resources they bind to.
// Static accessors hand those out for whichever engine is installed on the
// calling thread. nbio::async never sees any of this -- it asks the tag
// for a scheduler and nothing more.
//
// Member order matters twice. The multiplexer is declared first so the
// scheduler's idle hook can capture it, and the channels come last so they are
// destroyed first: every channel unregisters itself through
// multiplexer_.DeleteChannel() in its destructor, which requires a live
// multiplexer. Each channel references its resource, so each resource is
// declared before (and destroyed after) the channel bound to it.
class runtime {
   public:
    runtime(const runtime&) = delete;
    runtime& operator=(const runtime&) = delete;
    runtime(runtime&&) = delete;
    runtime& operator=(runtime&&) = delete;

    ~runtime() noexcept;

    // Installs a backend on the current thread. Throws if one is already
    // installed there.
    static void initialize(std::unique_ptr<core::Multiplexer> multiplexer);

    // Whether a backend has been installed on this thread.
    static bool is_initialized() { return static_cast<bool>(runtime_); }

    // The engine current on this thread. Throws when installed() is false.
    static runtime& instance();

    // ---- what nbio::async asks of a runtime tag ----
    static nbio::async::Scheduler& scheduler();
    static core::Multiplexer& multiplexer();

    // The channel carrying application-generated events. ConditionVariable
    // binds to it, so an application event travels the same path as a kernel
    // event: hand the waiter over, let the multiplexer dispatch it.
    static notification::EventNotifyChannel& notify_channel();

    // The standing channels owned by the engine.
    static time::SystemTimerChannel& timer_channel();

    // Lazily created: constructing the signal channel intercepts SIGINT and
    // SIGTERM, which must only happen if the application asks for it.
    static signal::SystemSignalChannel& signal_channel();

   private:
    explicit runtime(std::unique_ptr<core::Multiplexer> multiplexer);

    static std::function<void(bool)> make_idle_hook(core::Multiplexer& multiplexer);

    std::unique_ptr<core::Multiplexer> multiplexer_;
    nbio::async::Scheduler scheduler_;
    nbio::time::SystemTimer timer_;
    time::SystemTimerChannel timer_channel_;
    nbio::notification::EventNotifier notifier_;
    notification::EventNotifyChannel notify_channel_;
    std::unique_ptr<nbio::signal::SystemSignal> signal_;
    std::unique_ptr<signal::SystemSignalChannel> signal_channel_;

    static thread_local std::unique_ptr<runtime> runtime_;
};

// Creates the platform default backend
std::unique_ptr<core::Multiplexer> make_default_multiplexer();
}  // namespace nbio


