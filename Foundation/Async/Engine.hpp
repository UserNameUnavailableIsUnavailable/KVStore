#pragma once

#include <Foundation/Async/DefaultMultiplexer.hpp>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>

#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "SignalService.hpp"
#include "Task.hpp"
#include "TimerService.hpp"
#include "NotifyService.hpp"

namespace Foundation::Async::detail
{
class Engine
{
  struct Init
  {
    std::unique_ptr<Multiplexer> multiplexer;
  };
  public:
    explicit Engine(Init init);
    ~Engine() noexcept
    {
    }

    Engine(const Engine &) = delete;
    Engine &operator=(const Engine &) = delete;
    Engine(Engine &&) = delete;
    Engine &operator=(Engine &&) = delete;

    static void use_multiplexer(std::unique_ptr<Multiplexer> multiplexer)
    {
        if (Engine::engine_)
        {
            throw std::runtime_error("Async engine already settled down in this thread");
        }
        Init init{
            .multiplexer = std::move(multiplexer),
        };
        Engine::engine_ = std::make_unique<Engine>(std::move(init));
    }

    static Engine &instance()
    {
        if (engine_) return *engine_;
        Init init{
            .multiplexer = std::make_unique<DefaultMultiplexer>(),
        };
        engine_ = std::make_unique<Engine>(std::move(init));
        return *engine_;
    }

    Multiplexer &multiplexer() noexcept
    {
        return *multiplexer_;
    }
    const Multiplexer &multiplexer() const noexcept
    {
        return *multiplexer_;
    }
    Scheduler &scheduler() noexcept
    {
        return scheduler_;
    }
    const Scheduler &scheduler() const noexcept
    {
        return scheduler_;
    }
    TimerService &timer_service() noexcept
    {
        return timer_service_;
    }
    const TimerService &timer_service() const noexcept
    {
        return timer_service_;
    }
    NotifyService &notify_service() noexcept
    {
        return notify_service_;
    }
    const NotifyService &notify_service() const noexcept
    {
        return notify_service_;
    }

    SignalService &signal_service() noexcept;

    template <typename T> static CoroutineToken spawn(Task<T> task)
    {
        auto &scheduler = Engine::instance().scheduler();
        return scheduler.spawn(std::move(task));
    }

    static void run(Task<void> main)
    {
        std::exception_ptr error;
        auto &engine = instance();
        auto &scheduler = engine.scheduler();
        scheduler.spawn(guard_exception(std::move(main), error));
        while (scheduler.has_alive())
        {
            scheduler.run();
        }
        if (error)
        {
            std::rethrow_exception(error);
        }
    }

  private:

    static std::function<void(bool)> make_idle_hook(Multiplexer &multiplexer)
    {
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

    static Task<void> guard_exception(Task<void> main, std::exception_ptr &exception)
    {
        try
        {
            co_await std::move(main);
        }
        catch (...)
        {
            exception = std::current_exception();
        }
    }

    void initialize_signal_service_on_demand();

    std::unique_ptr<Multiplexer> multiplexer_;
    Scheduler scheduler_;
    TimerService timer_service_;
    NotifyService notify_service_;
    // NOTE: Once signal service is created, SIGINT & SIGTERM will be intercepted.
    // If no one handles these two signals, then the process will not exit on signals.
    // We only initialize signal service on demand.
    std::unique_ptr<SignalService> signal_service_;
    static thread_local std::unique_ptr<Engine> engine_;
};
} // namespace Foundation::Async::detail
