#pragma once

#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <cstdint>

#include "Types.hpp"

namespace Foundation::NBIO {
class Multiplexer;
class ChannelBase;

// Something the multiplexer watches, bound to one event source. Strictly owned by
// whatever made it -- a stream, a session, a service -- and never moved: the
// multiplexer holds a pointer to the channel, so it has to stay where it was
// registered.
class ChannelBase {
   public:
    explicit ChannelBase(ChannelType type, std::uintptr_t native_handle, Multiplexer& multiplexer,
                         Foundation::Async::Scheduler& scheduler)
        : type_(type), native_handle_(native_handle), multiplexer_(multiplexer), scheduler_(scheduler) {}

    ChannelBase(const ChannelBase&) = delete;
    ChannelBase& operator=(const ChannelBase&) = delete;
    // IMPORTANT: a registered channel must never be moved, because the
    // multiplexer holds a pointer to it.
    ChannelBase(ChannelBase&&) = delete;
    ChannelBase& operator=(ChannelBase&&) = delete;

    ~ChannelBase() noexcept = default;

    ChannelType type() const noexcept { return type_; }

    std::uintptr_t native_handle() const noexcept { return native_handle_; }

    Multiplexer& multiplexer() noexcept { return multiplexer_; }

    Foundation::Async::Scheduler& scheduler() noexcept { return scheduler_; }

    const Foundation::Async::Scheduler& scheduler() const noexcept { return scheduler_; }

    const Multiplexer& multiplexer() const noexcept { return multiplexer_; }

    bool armed() const noexcept { return armed_; }

    // Arm a channel with its associated event. Arming says "this channel has work
    // for the backend"; a channel with none disarms itself. Registration is the
    // whole of arming now that every channel is dedicated to one event: there is
    // nothing to update, only to add and to remove.
    void arm() {
        if (armed_) return;
        armed_ = true;
        multiplexer_.add_channel(this);
    }

    // Disarm a channel: its events will no longer be reported by the multiplexer.
    void disarm() {
        if (!armed_) return;
        armed_ = false;
        multiplexer_.delete_channel(this);
    }

   protected:
    const ChannelType type_;
    const std::uintptr_t native_handle_;
    Multiplexer& multiplexer_;
    Foundation::Async::Scheduler& scheduler_;
    bool armed_{false};
};

template <typename C>
class Channel : public ChannelBase {
   public:
    using ChannelBase::ChannelBase;

    auto submit() { return static_cast<C*>(this)->submit(); }

    auto submit() const { return static_cast<const C*>(this)->submit(); }
};
}  // namespace Foundation::NBIO
