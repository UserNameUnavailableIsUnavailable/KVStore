#include "Engine.hpp"

#include <Foundation/NBIO/NBIO.hpp>
#include <chrono>
#include <stdexcept>
#include <utility>

#if !defined(__linux__)
#error "NBIO is only implemented for Linux"
#endif

#include "EpollMultiplexer.hpp"

namespace Foundation::NBIO
{
thread_local std::unique_ptr<Engine> Engine::engine_{};

Engine::Engine(std::unique_ptr<Multiplexer> multiplexer)
    : multiplexer_(std::move(multiplexer)), scheduler_(make_idle_hook(*multiplexer_)),
      timer_channel_(timer_, *multiplexer_, scheduler_), notify_channel_(notifier_, *multiplexer_, scheduler_)
{
}

Engine::~Engine() noexcept = default;

std::function<void(bool)> Engine::make_idle_hook(Multiplexer &multiplexer)
{
    // The scheduler parks here whenever it has nothing to run: the multiplexer
    // *is* the idle coroutine.
    return [&multiplexer](bool blocking) {
        if (blocking)
        {
            multiplexer.run();
        }
        else
        {
            multiplexer.run_for(std::chrono::milliseconds(0));
        }
    };
}

void Engine::initialize(std::unique_ptr<Multiplexer> multiplexer)
{
    if (is_initialized())
    {
        throw std::runtime_error("NBIO backend already initialized on this thread");
    }
    engine_ = std::unique_ptr<Engine>(new Engine(std::move(multiplexer)));
}

Engine &Engine::instance()
{
    if (!is_initialized())
    {
        initialize(std::make_unique<EpollMultiplexer>());
    }
    return *engine_;
}

Foundation::Async::Scheduler &Engine::scheduler()
{
    return instance().scheduler_;
}

Multiplexer &Engine::multiplexer()
{
    return *instance().multiplexer_;
}

NotifyChannel &Engine::notify_channel()
{
    return instance().notify_channel_;
}

TimerChannel &Engine::timer_channel()
{
    return instance().timer_channel_;
}

SignalChannel &Engine::signal_channel()
{
    auto &self = instance();
    if (!self.signal_channel_)
    {
        self.signal_ = std::make_unique<Foundation::Core::Signal>();
        self.signal_channel_ =
            std::make_unique<SignalChannel>(*self.signal_, *self.multiplexer_, self.scheduler_);
    }
    return *self.signal_channel_;
}

std::unique_ptr<Multiplexer> make_default_multiplexer()
{
    return std::make_unique<EpollMultiplexer>();
}
} // namespace Foundation::NBIO
