#pragma once

#include <cstddef>
#include <cstdint>

namespace NBIO::RDMA {
// What a packet is: a bitmask, because one packet can be more than one thing. An
// acknowledgement rides on a payload packet whenever there is one to ride on, which is
// what keeps a link that is carrying data from also paying a message per packet to say
// that it arrived.
enum class RdmaPacketType : std::uint8_t {
    kMeta = 1,     // the two ends saying what they can take
    kAck = 2,      // this packet's `acknowledge` is worth reading
    kPayload = 4,  // the payload is the caller's bytes, in order
};

constexpr RdmaPacketType operator|(RdmaPacketType lhs, RdmaPacketType rhs) noexcept {
    return static_cast<RdmaPacketType>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
}

constexpr bool operator&(RdmaPacketType lhs, RdmaPacketType rhs) noexcept {
    return (static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs)) != 0;
}

struct RdmaHeader {
    std::uint64_t sequence;
    std::uint64_t acknowledge;
    RdmaPacketType type;
    std::uint8_t reserved[7];
};

static_assert(sizeof(RdmaHeader) == 24, "the payload has to start eight-byte aligned");

// What a packet of `length` bytes carried, or nothing when it is too short to hold a
// header at all.
inline std::size_t PacketPayloadBytes(std::size_t length) noexcept {
    return length > sizeof(RdmaHeader) ? length - sizeof(RdmaHeader) : 0;
}
}  // namespace NBIO::RDMA


