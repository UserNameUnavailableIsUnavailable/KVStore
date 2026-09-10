#pragma once

#include <bit>
#include <cstdint>
#include <type_traits>

namespace Foundation
{
namespace detail_Byte_hpp
{
constexpr std::uint16_t Swap16(std::uint16_t value) noexcept
{
    return ((value & 0x00FF) << 8U) | ((value & 0xFF00) >> 8U);
}

constexpr std::uint32_t Swap32(std::uint32_t value) noexcept
{
    auto b1 = (value & 0x000000FFU) << 24U;
    auto b2 = (value & 0x0000FF00U) << 8U;
    auto b3 = (value & 0x00FF0000) >> 8U;
    auto b4 = (value & 0xFF000000) >> 24U;
    return (b1 | b2 | b3 | b4);
}

constexpr std::uint64_t Swap64(std::uint64_t value) noexcept
{
    auto b1 = (value & 0x00000000000000FFULL) << 56U;
    auto b2 = (value & 0x000000000000FF00ULL) << 40U;
    auto b3 = (value & 0x0000000000FF0000ULL) << 24U;
    auto b4 = (value & 0x00000000FF000000ULL) << 8U;
    auto b5 = (value & 0x000000FF00000000ULL) >> 8U;
    auto b6 = (value & 0x0000FF0000000000ULL) >> 24U;
    auto b7 = (value & 0x00FF000000000000ULL) >> 40U;
    auto b8 = (value & 0xFF00000000000000ULL) >> 56U;
    return b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
}
}

template <typename T>
requires (std::is_integral_v<T> && !std::is_same_v<std::remove_cv_t<T>, bool>)
constexpr T swap_endian(T value) noexcept
{
    if constexpr (sizeof(T) == 1)
    {
        return value;
    }
    using Unsigned = std::make_unsigned_t<T>;
    const auto raw = static_cast<Unsigned>(value);
    if constexpr (sizeof(T) == 2)
    {
        return static_cast<T>(detail_Byte_hpp::Swap16(static_cast<std::uint16_t>(raw)));
    }
    else if constexpr (sizeof(T) == 4)
    {
        return static_cast<T>(detail_Byte_hpp::Swap32(static_cast<std::uint32_t>(raw)));
    }
    else if constexpr (sizeof(T) == 8)
    {
        return static_cast<T>(detail_Byte_hpp::Swap64(static_cast<std::uint64_t>(raw)));
    }
    else
    {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                      "sizeof(T) must be 1, 2, 4, or 8");
    }
}

// convert native-endian value to little endian
template <typename T>
constexpr T to_little_endian(T value) noexcept
{
    if constexpr (std::endian::native == std::endian::little)
    {
        return value;
    }
    else
    {
        return swap_endian(value);
    }
}

// convert small-endian value to native endian
template <typename T>
constexpr T from_little_endian(T value) noexcept
{
    if constexpr (std::endian::native == std::endian::little)
    {
        return value;
    }
    else
    {
        return swap_endian(value);
    }
}

// convert native-endian value to big endian
template <typename T>
constexpr T to_big_endian(T value) noexcept
{
    if constexpr (std::endian::native == std::endian::big)
    {
        return value;
    }
    else
    {
        return swap_endian(value);
    }
}

// convert big-endian value to native endian
template <typename T>
constexpr T from_big_endian(T value) noexcept
{
    if constexpr (std::endian::native == std::endian::big)
    {
        return value;
    }
    else
    {
        return swap_endian(value);
    }
}
} // namespace Foundation