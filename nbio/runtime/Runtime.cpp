#include <nbio/runtime/Runtime.hpp>
#include <chrono>
#include <stdexcept>
#include <utility>

#if !defined(__linux__)
#error "nbio is only implemented for Linux"
#endif

#include <nbio/core/EpollMultiplexer.hpp>

namespace nbio {
thread_local std::unique_ptr<runtime> runtime::runtime_{};

runtime::runtime(std::unique_ptr<core::Multiplexer> multiplexer)
    : multiplexer_(std::move(multiplexer)),
      scheduler_(make_idle_hook(*multiplexer_)),
      timer_channel_(timer_, *multiplexer_, scheduler_),
      notify_channel_(notifier_, *multiplexer_, scheduler_) {}

runtime::~runtime() noexcept = default;

std::function<void(bool)> runtime::make_idle_hook(core::Multiplexer& multiplexer) {
    // The scheduler Parks here whenever it has nothing to run: the multiplexer
    // *is* the idle coroutine.
    return [&multiplexer](bool blocking) {
        if (blocking) {
            multiplexer.Run();
        } else {
            multiplexer.RunFor(std::chrono::milliseconds(0));
        }
    };
}

void runtime::initialize(std::unique_ptr<core::Multiplexer> multiplexer) {
    if (is_initialized()) {
        throw std::runtime_error("nbio backend already initialized on this thread");
    }
    runtime_ = std::unique_ptr<runtime>(new runtime(std::move(multiplexer)));
}

runtime& runtime::instance() {
    if (!is_initialized()) {
        initialize(std::make_unique<core::EpollMultiplexer>());
    }
    return *runtime_;
}

nbio::async::Scheduler& runtime::scheduler() { return instance().scheduler_; }

core::Multiplexer& runtime::multiplexer() { return *instance().multiplexer_; }

notification::EventNotifyChannel& runtime::notify_channel() { return instance().notify_channel_; }

time::SystemTimerChannel& runtime::timer_channel() { return instance().timer_channel_; }

signal::SystemSignalChannel& runtime::signal_channel() {
    auto& self = instance();
    if (!self.signal_channel_) {
        self.signal_ = std::make_unique<nbio::signal::SystemSignal>();
        self.signal_channel_ =
            std::make_unique<signal::SystemSignalChannel>(*self.signal_, *self.multiplexer_, self.scheduler_);
    }
    return *self.signal_channel_;
}

std::unique_ptr<core::Multiplexer> make_default_multiplexer() { return std::make_unique<core::EpollMultiplexer>(); }
}  // namespace nbio


