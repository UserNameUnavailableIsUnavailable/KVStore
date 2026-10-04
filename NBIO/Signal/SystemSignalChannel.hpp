#pragma once

#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Signal/SystemSignal.hpp>
#include <NBIO/Core/Channel.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <coroutine>
#include <cstddef>
#include <list>

namespace NBIO::Signal {
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

    // cancellation detach: if this frame is destroyed while still parked,
    // drop the registration so a later signal never resumes a dead handle.
    ~SystemSignalAwaiter() noexcept = default;
};

// A channel over a shared SystemSignal eventfd. A signal writes to every registered
// SystemSignal instance; this channel then resumes every coroutine waiting on this
// engine's signal channel.
class SystemSignalChannel final : public NBIO::Core::Channel<SystemSignalChannel> {
   public:
    using Payload = detail::PollPayload<SystemSignalChannel>;

    SystemSignalChannel(NBIO::Signal::SystemSignal& signal, NBIO::Core::Multiplexer& multiplexer,
                        NBIO::Async::Scheduler& scheduler);
    ~SystemSignalChannel();

    // The operation this channel wants from the backend is a one-shot poll; the
    // payload carries only whether one is already out there.
    Payload& submit();
    void complete();

    void park(NBIO::Async::Coroutine coroutine);

    const Signal::SystemSignal& signal() const noexcept { return signal_; }

    Signal::SystemSignal& signal() noexcept { return signal_; }

    SystemSignalAwaiter wait() noexcept { return SystemSignalAwaiter{*this}; }

   private:
    NBIO::Signal::SystemSignal& signal_;
    std::list<NBIO::Async::Coroutine> waiters_;
    Payload payload_{};
};

template <typename PromiseType>
void SystemSignalAwaiter::await_suspend(std::coroutine_handle<PromiseType> handle) noexcept {
    auto coroutine = NBIO::Async::Coroutine::from_handle(handle);
    channel.arm();
    channel.park(std::move(coroutine));
}
}  // namespace NBIO::Signal




