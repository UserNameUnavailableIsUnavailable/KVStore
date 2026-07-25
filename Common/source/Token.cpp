#include "Common/Token.hpp"

#include <optional>

static inline bool is_continuation_byte(std::uint8_t byte)
{
    return (byte & 0b1100'0000) == 0b1000'0000;
}

namespace Macrohard
{
std::optional<CodePoint> ResolveNextCodePoint(std::string_view s)
{
    CodePoint cp;
    if (s.empty())
    {
        return std::nullopt;
    }
    // determine the length of the code point based on the first byte
    std::uint8_t first_byte = static_cast<std::uint8_t>(s[0]);
    if (first_byte <= 0x7F)
    {
        cp.length = 1;
        cp.representation = {
            static_cast<std::uint8_t>(first_byte), 0, 0, 0
        };
    }
    else if ((0b1110'0000 & first_byte) == 0b1100'0000) // 2-byte sequence
    {
        if (s.size() < 2)
        {
            return std::nullopt; // not enough bytes for a valid code point
        }
        auto second_byte = static_cast<std::uint8_t>(s[1]);
        if (!is_continuation_byte(second_byte))
        {
            return std::nullopt; // invalid continuation byte
        }
        cp.length = 2;
        cp.representation = {
            static_cast<std::uint8_t>(first_byte),
            static_cast<std::uint8_t>(second_byte),
            0,
            0
        };
    }
    else if ((0b1111'0000 & first_byte) == 0b1110'0000) // 3-byte sequence
    {
        if (s.size() < 3)
        {
            return std::nullopt; // not enough bytes for a valid code point
        }
        auto second_byte = static_cast<std::uint8_t>(s[1]);
        auto third_byte = static_cast<std::uint8_t>(s[2]);
        if (!is_continuation_byte(second_byte) || !is_continuation_byte(third_byte))
        {
            return std::nullopt; // invalid continuation byte
        }
        cp.length = 3;
        cp.representation = {
            static_cast<std::uint8_t>(first_byte),
            static_cast<std::uint8_t>(second_byte),
            static_cast<std::uint8_t>(third_byte),
            0
        };
    }
    else if ((0b1111'1000 & first_byte) == 0b1111'0000) // 4-byte sequence
    {
        if (s.size() < 4)
        {
            return std::nullopt; // not enough bytes for a valid code point
        }
        auto second_byte = static_cast<std::uint8_t>(s[1]);
        auto third_byte = static_cast<std::uint8_t>(s[2]);
        auto fourth_byte = static_cast<std::uint8_t>(s[3]);
        if (!is_continuation_byte(second_byte) || !is_continuation_byte(third_byte) || !is_continuation_byte(fourth_byte))
        {
            return std::nullopt; // invalid continuation byte
        }
        cp.length = 4;
        cp.representation = {
            static_cast<std::uint8_t>(first_byte),
            static_cast<std::uint8_t>(second_byte),
            static_cast<std::uint8_t>(third_byte),
            static_cast<std::uint8_t>(fourth_byte)
        };
    }
    else
    {
        return std::nullopt; // invalid first byte for UTF-8
    }

    return cp;
}
}
