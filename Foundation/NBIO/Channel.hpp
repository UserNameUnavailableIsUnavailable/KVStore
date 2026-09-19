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

    // There is deliberately no submission protocol here. Each channel keeps what
    // fits it: a queue of waits with a batch to hand over (receive, send, read,
    // write), one wait at a time (accept), or no per-operation wait at all (timer,
    // notifier, signal, RDMA stream). The multiplexer switches on `type()` anyway,
    // so it calls those methods on the concrete channel rather than the base class
    // pretending every channel has them.

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
        armed_ = true;
        multiplexer_.update_channel(this);
    }

    // Disarm a channel: its events will no longer be reported by the multiplexer.
    void disarm()
    {
        armed_ = false;
        multiplexer_.update_channel(this);
    }

  protected:
    // A readiness backend submits the moment a wait is prepared, so the channel
    // hands its own work over and the multiplexer only has to watch the
    // descriptor. A completion backend runs a submission phase, and the
    // multiplexer is what runs it.
    bool submits_immediately() const noexcept
    {
        return multiplexer_.submits_immediately();
    }

    const ChannelType type_;
	const std::uintptr_t native_handle_;
    Multiplexer &multiplexer_;
    Foundation::Async::Scheduler &scheduler_;
    bool armed_{false};
};
} // namespace Foundation::NBIO
