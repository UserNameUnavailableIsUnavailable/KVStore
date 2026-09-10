#pragma once

#include <spdlog/spdlog.h>

#include "Multiplexer.hpp"
#include "Scheduler.hpp"
#include "Types.hpp"

namespace Foundation::Async
{
class Channel;
using IOHandler = void (*)(Channel *);

class Channel
{
  public:
    using Handle = int;
    constexpr static Handle kInvalidHandle = -1;

    explicit Channel(ChannelType type, Handle handle, Multiplexer &multiplexer, Scheduler &scheduler)
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

    // Called by the multiplexer once the channel's job has been filled: by the
    // handler for readiness backends, or by the multiplexer itself for
    // completion backends. The channel only does job control -- if the job
    // reached a conclusive status, resume the waiting coroutine.
    virtual void on_event() = 0;

    bool armed() const noexcept
    {
        return armed_;
    }
    bool &armed() noexcept
    {
        return armed_;
    }
    void arm()
    {
        // Mark armed before syncing so readiness backends include this channel
        // in CombinedFlags(fd) during ADD/MOD.
        armed_ = true; // FIXME: do not rely on armed = true inside multiplexer!
        multiplexer_.update_channel(this);
    }
    void disarm()
    {
        armed_ = false;
        multiplexer_.update_channel(this);
    }

    Handle get_native_handle() const noexcept
    {
        return handle_;
    }

  protected:
    const ChannelType type_;
    const Handle handle_;
    Multiplexer &multiplexer_;
    Scheduler &scheduler_;
    IOHandler handler_{nullptr};
    bool armed_{false};
};
} // namespace Foundation::Async
