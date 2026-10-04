#pragma once

#include <nbio/async/Scheduler.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/signal/SystemSignal.hpp>
#include <nbio/core/Channel.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <coroutine>
#include <cstddef>
#include <list>

namespace nbio::signal {
namespace detail {
template <typename C>
class PollPayload {
   public:
    bool wants_poll() const noexcept { return !submitted_; }
    void take_poll() noexcept { submitted_ = true; }
    void release_poll() noexcept { submitted_ = false; }
    bool outstanding() const noexcept { return submitted_; }

   private:
    bool submitted_{false};
};
}  // namespace detail

class SystemSignalChannel;

struct SystemSignalAwaiter {
    SystemSignalChannel& channel;

    bool await_ready() const noexcept { return false; }

    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType> handle) noexcept;

    void await_resume() noexcept {}

    // cancellation detach: if this frame is destroyed while still Parked,
    // drop the registration so a later signal never resumes a dead handle.
    ~SystemSignalAwaiter() noexcept = default;
};

// A channel over a shared SystemSignal eventfd. A signal writes to every registered
// SystemSignal instance; this channel then resumes every coroutine waiting on this
// engine's signal channel.
class SystemSignalChannel final : public nbio::core::Channel<SystemSignalChannel> {
   public:
    using Payload = detail::PollPayload<SystemSignalChannel>;

    SystemSignalChannel(nbio::signal::SystemSignal& signal, nbio::core::Multiplexer& multiplexer,
                        nbio::async::Scheduler& scheduler);
    ~SystemSignalChannel();

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& Submit();
    void Complete();

    void Park(nbio::async::Coroutine coroutine);

    const signal::SystemSignal& signal() const noexcept { return signal_; }

    signal::SystemSignal& signal() noexcept { return signal_; }

    SystemSignalAwaiter wait() noexcept { return SystemSignalAwaiter{*this}; }

   private:
    nbio::signal::SystemSignal& signal_;
    std::list<nbio::async::Coroutine> waiters_;
    Payload payload_{};
};

template <typename PromiseType>
void SystemSignalAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) noexcept {
    auto coroutine = nbio::async::Coroutine::FromHandle(handle);
    channel.Arm();
    channel.Park(std::move(coroutine));
}
}  // namespace nbio::signal




