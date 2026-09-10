#include "Engine.hpp"

#include <Foundation/Async/Multiplexer.hpp>
#include <Foundation/Async/SignalService.hpp>
#include <memory>

#include "DefaultMultiplexer.hpp"

namespace Foundation::Async::detail
{
Engine::Engine(Init init)
    : multiplexer_(std::move(init.multiplexer)), scheduler_(make_idle_hook(*multiplexer_)),
      timer_service_(*multiplexer_, scheduler_),
      notify_service_(*multiplexer_, scheduler_)
{
}

thread_local std::unique_ptr<Engine> Engine::engine_{};

SignalService &Engine::signal_service() noexcept
{
    if (!signal_service_)
    {
        signal_service_ = std::make_unique<SignalService>(*multiplexer_, scheduler_);
    }
    return *signal_service_;
}

} // namespace Foundation::Async::detail
