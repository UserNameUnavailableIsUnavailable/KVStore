#pragma once

#include <nbio/async/Scheduler.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <cstdint>

#include "Types.hpp"

namespace nbio::core {
class Multiplexer;
class ChannelBase;

// Something the multiplexer watches, bound to one event source. Strictly owned by
// whatever made it -- a stream, a session, a service -- and never moved: the
// multiplexer holds a pointer to the channel, so it has to stay where it was
// registered.
class ChannelBase {
   public:
    explicit ChannelBase(ChannelType type, std::uintptr_t native_handle, Multiplexer& multiplexer,
                         nbio::async::Scheduler& scheduler)
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

    nbio::async::Scheduler& scheduler() noexcept { return scheduler_; }

    const nbio::async::Scheduler& scheduler() const noexcept { return scheduler_; }

    const Multiplexer& multiplexer() const noexcept { return multiplexer_; }

    bool is_armed() const noexcept { return armed_; }

    // Arm a channel with its associated event. Arming says "this channel has work
    // for the backend"; a channel with none disarms itself. Registration is the
    // whole of arming now that every channel is dedicated to one event: there is
    // nothing to update, only to add and to remove.
    void Arm() {
        if (armed_) return;
        armed_ = true;
        multiplexer_.AddChannel(this);
    }

    // Disarm a channel: its events will no longer be reported by the multiplexer.
    void Disarm() {
        if (!armed_) return;
        armed_ = false;
        multiplexer_.DeleteChannel(this);
    }

   protected:
    const ChannelType type_;
    const std::uintptr_t native_handle_;
    Multiplexer& multiplexer_;
    nbio::async::Scheduler& scheduler_;
    bool armed_{false};
};

template <typename C>
class Channel : public ChannelBase {
   public:
    using ChannelBase::ChannelBase;

    auto Submit() { return static_cast<C*>(this)->Submit(); }

    auto Submit() const { return static_cast<const C*>(this)->Submit(); }
};
}  // namespace nbio::core


