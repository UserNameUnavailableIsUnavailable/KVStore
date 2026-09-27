#pragma once

#include <cstddef>
#include <cstdint>

namespace Foundation::Core
{
// What a packet is: a bitmask, because one packet can be more than one thing. An
// acknowledgement rides on a payload packet whenever there is one to ride on, which is
// what keeps a link that is carrying data from also paying a message per packet to say
// that it arrived.
enum class RdmaPacketType : std::uint8_t
{
    kMeta = 1,    // the two ends saying what they can take
    kAck = 2,     // this packet's `acknowledge` is worth reading
    kPayload = 4, // the payload is the caller's bytes, in order
};

constexpr RdmaPacketType operator|(RdmaPacketType lhs, RdmaPacketType rhs) noexcept
{
    return static_cast<RdmaPacketType>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
}

constexpr bool operator&(RdmaPacketType lhs, RdmaPacketType rhs) noexcept
{
    return (static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs)) != 0;
}

// The header every packet starts with, at the front of the chunk it arrives in: the
// payload the caller is handed begins right after it, so one chunk carries both the
// framing and the bytes and neither costs an allocation of its own.
//
// The two numbers are the whole of the flow control. Every packet is numbered in its
// sender's stream -- acks included, because an ack is a packet and lands in a receive
// like any other -- and `acknowledge` says which of the peer's numbers this side has
// finished with. What bounds a sender is then one subtraction: the packets it has sent
// that the peer has not acknowledged are exactly the ones sitting in the peer's posted
// receives, so holding that count inside what the peer said it can take is what keeps
// the queue pair alive.
//
// It is 24 bytes rather than 20 so the payload starts eight-byte aligned, which is
// where a read of it wants to be; the seven bytes after `type` are there for that.
struct RdmaPacket
{
    // Zero means nothing has been taken yet, and the first packet a side sends is
    // numbered one, so that "nothing" and "packet zero" are not the same number.
    std::uint64_t sequence;
    std::uint64_t acknowledge;
    RdmaPacketType type;
    std::uint8_t reserved[7];
    char payload[];
};

static_assert(sizeof(RdmaPacket) == 24, "the payload has to start eight-byte aligned");

// What a packet of `length` bytes carried, or nothing when it is too short to hold a
// header at all.
inline std::size_t PacketPayloadBytes(std::size_t length) noexcept
{
    return length > sizeof(RdmaPacket) ? length - sizeof(RdmaPacket) : 0;
}
} // namespace Foundation::Core