#pragma once

#include <cstdint>
#include <span>

namespace KV::checksum {
std::uint32_t CRC32(std::span<const char> bytes);
std::uint64_t RedisCRC64(std::span<const char> bytes);
std::uint64_t RedisCRC64(std::span<const std::uint8_t> bytes);
}  // namespace KV::checksum