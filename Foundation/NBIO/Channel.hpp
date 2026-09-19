#pragma once

#include <cstdint>
#include <spdlog/spdlog.h>

#include <Foundation/Async/Scheduler.hpp>
#include <cstddef>

#include "Multiplexer.hpp"
#include "Types.hpp"

namespace Foundation::NBIO
{
class Channel
{
  public:

    explicit Channel(ChannelType type, std::uintptr_t native_handle, Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
        : type_(type), native_handle_(native_handle), multiplexer_(multiplexer), scheduler_(scheduler)
    {
    }

    Channel(const Channel &) = delete;
    Channel &operator=(const Channel &) = delete;
    // IMPORTANT: a registered channel must never be moved, because the
    // multiplexer holds a pointer to it.
    Channel(Channel &&) = delete;
    Channel &operator=(Channel &&) = delete;

    ~Channel() noexcept = default;

    ChannelType type() const noexcept
    {
        return type_;
    }

    std::uintptr_t native_handle() const noexcept
    {
        return native_handle_;
    }

    // There is deliberately no submission protocol here. A channel that carries
    // per-operation waits -- receive, send, read, write, accept -- is *batchable*,
    // and every one of them keeps the same four methods, declared with the types
    // that fit it (a `msghdr`, a span of `iovec`s and an offset, one wait):
    //
    //   submit_jobs()       Moves the prepared prefix over as one operation and
    //                       answers what the backend submits with, or nothing when
    //                       there is nothing to submit. A channel never submits
    //                       anything itself.
    //   advance_job()       One operation's outcome, spread over the jobs it
    //                       covered: every job it answered moves to the completed
    //                       queue, and what it did not answer keeps its place.
    //   complete_jobs()     Concludes the operation: the jobs it did not answer go
    //                       back to the front of the prepared queue, in their
    //                       original order, so the next operation starts where this
    //                       one stopped.
    //   handle_completion() Hands the completed jobs' waiters to the scheduler,
    //                       then arms the channel while anything is still prepared
    //                       and disarms it otherwise.
    //
    // The backend drives them: submit_jobs() where it submits, advance_job() once
    // per operation it completes, then complete_jobs() -- and, once every
    // completion for that channel has been advanced, handle_completion(). Waking
    // last is what makes the batch safe to write into: a resumed coroutine is free
    // to prepare more work, which must not happen while a completion is still
    // being spread over the batch it is about to join.
    //
    // The multiplexer switches on `type()` to reach them, so the base class never
    // pretends every channel has them: a timer, a notifier, a signal and an RDMA
    // stream carry no per-operation wait at all and keep to their own protocol.

    Multiplexer &multiplexer() noexcept
    {
        return multiplexer_;
    }

    Foundation::Async::Scheduler &scheduler() noexcept
    {
        return scheduler_;
    }

    const Foundation::Async::Scheduler &scheduler() const noexcept
    {
        return scheduler_;
    }

    const Multiplexer &multiplexer() const noexcept
    {
        return multiplexer_;
    }

    bool armed() const noexcept
    {
        return armed_;
    }

    // Arm a channel with its associated event. Arming says "this channel has work
    // for the backend"; a channel with none disarms itself.
    void arm()
    {
        if (armed_) return;
        armed_ = true;
        multiplexer_.update_channel(this);
    }

    // Disarm a channel: its events will no longer be reported by the multiplexer.
    void disarm()
    {
        if (!armed_) return;
        armed_ = false;
        multiplexer_.update_channel(this);
    }

  protected:
    const ChannelType type_;
	const std::uintptr_t native_handle_;
    Multiplexer &multiplexer_;
    Foundation::Async::Scheduler &scheduler_;
    bool armed_{false};
};
} // namespace Foundation::NBIO
