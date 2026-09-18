#pragma once

#include <spdlog/spdlog.h>

#include <Foundation/Async/Scheduler.hpp>
#include <utility>

#include "Multiplexer.hpp"
#include "Types.hpp"

namespace Foundation::NBIO
{
class Channel;
class Channel
{
  public:
	using IOHandler = void (*)(Channel *);

    using Handle = int;
    constexpr static Handle kInvalidHandle = -1;

    explicit Channel(ChannelType type, Handle handle, Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
        : type_(type), handle_(handle), multiplexer_(multiplexer), scheduler_(scheduler)
    {
    }

    Channel(const Channel &) = delete;
    Channel &operator=(const Channel &) = delete;
    // IMPORTANT: a registered channel must never be moved, because the
    // multiplexer holds a pointer to it.
    Channel(Channel &&) = delete;
    Channel &operator=(Channel &&) = delete;

    virtual ~Channel() noexcept = default;

    ChannelType type() const noexcept
    {
        return type_;
    }

    // The multiplexer installs the backend handler at registration time.
    void on_event(IOHandler handler) noexcept
    {
        handler_ = handler;
    }

    // The receive side, called by the multiplexer once the channel's job has
    // been filled: by the handler for readiness backends, or by the multiplexer
    // itself for completion backends. The channel only does job control -- if
    // the job reached a conclusive status, resume the waiting coroutine.
    //
    // There is deliberately no send side here. Whether a channel can be
    // triggered at all is a property of its type, not of the base: kernel-driven
    // channels (receive, send, listen, read, write, timer, signal) have no
    // sender at all, so an application-triggered event is expressed by a
    // concrete channel that provides one (see NotifyChannel).
    //
    // A channel drives ONE job at a time and, unless it says otherwise, wants one
    // coroutine: the job, the offset it was armed with and the parked coroutine
    // all live on the channel, so a second coroutine starting the same operation
    // would write over the first one's job and drop the parked coroutine that
    // nothing else refers to. A channel that a shared resource makes reachable
    // from more than one coroutine therefore has to queue its work --
    // WriteChannel is the one that does, because every client session appends to
    // the same file through it. SignalChannel and NotifyChannel take many
    // waiters by design; those wait on an event rather than for a job.
    virtual void handle_event() = 0;

    Handle native_handle() const noexcept
    {
        return handle_;
    }

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

    // Arm a channel with its associated event.
    void arm()
    {
        if (!std::exchange(armed_, true))
        {
            multiplexer_.update_channel(this);
        }
    }

    // Disarm a channel. Its events will no longer be reported by the multiplexer.
    // Called in multiplexer, after event triggers and before handle_event.
    void disarm()
    {
        if (std::exchange(armed_, false))
        {
            multiplexer_.update_channel(this);
        }
    }

  protected:
    const ChannelType type_;
    const Handle handle_;
    Multiplexer &multiplexer_;
    Foundation::Async::Scheduler &scheduler_;
    IOHandler handler_{nullptr};
    bool armed_{false};
};
} // namespace Foundation::NBIO
