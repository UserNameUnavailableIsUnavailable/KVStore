#include <checksum/checksum.hpp>

#include <CRC.h>

namespace KV::checksum {
std::uint32_t CRC32(std::span<const char> bytes) {
    return static_cast<std::uint32_t>(CRC::Calculate(bytes.data(), bytes.size(), CRC::CRC_32()));
}

std::uint64_t RedisCRC64(std::span<const char> bytes) {
    constexpr CRC::Parameters<std::uint64_t, 64> parameters{0xAD93D23594C935A9ULL, 0, 0, true, true};
    return CRC::Calculate(bytes.data(), bytes.size(), parameters);
}

std::uint64_t RedisCRC64(std::span<const std::uint8_t> bytes) {
    constexpr CRC::Parameters<std::uint64_t, 64> parameters{0xAD93D23594C935A9ULL, 0, 0, true, true};
    return CRC::Calculate(bytes.data(), bytes.size(), parameters);
}
}  // namespace KV::checksum