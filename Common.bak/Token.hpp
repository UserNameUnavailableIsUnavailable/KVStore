#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <array>

namespace KV
{
// UTF-8 knowledge
// A code point is a Unicode character represented as a 32-bit integer.
// prefix: 0x00000000 to 0x0000007F (ASCII) - 1 byte
// prefix: 0x00000080 to 0x000007FF - 2 bytes
// prefix: 0x00000800 to 0x0000FFFF - 3 bytes
// prefix: 0x00010000 to 0x0010FFFF - 4 bytes

struct CodePoint
{
    std::array<uint8_t, 4> representation; // UTF-8 representation of the code point
    std::uint32_t length; // number of bytes in UTF-8 encoding
};

std::optional<CodePoint> resolveNextCodePoint(std::string_view s);

} // namespace KV
