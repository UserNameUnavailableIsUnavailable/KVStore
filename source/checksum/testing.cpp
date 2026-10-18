#include <gtest/gtest.h>

#include <checksum/checksum.hpp>

#include <array>
#include <cstdint>
#include <span>

TEST(Checksum, CRC32MatchesStandardCheckVector) {
    constexpr std::array bytes{'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    EXPECT_EQ(KV::checksum::CRC32(bytes), 0xCBF43926U);
}

TEST(Checksum, RedisCRC64MatchesRedisCheckVector) {
    constexpr std::array bytes{'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    EXPECT_EQ(KV::checksum::RedisCRC64(bytes), 0xE9C6D914C4B8D9CAULL);
}

TEST(Checksum, CRCsHandleEmptyAndBinaryInput) {
    EXPECT_EQ(KV::checksum::CRC32(std::span<const char>{}), 0U);
    EXPECT_EQ(KV::checksum::RedisCRC64(std::span<const char>{}), 0U);

    constexpr std::array<std::uint8_t, 4> binary{0, 1, 0x80, 0xff};
    EXPECT_EQ(KV::checksum::RedisCRC64(binary),
              KV::checksum::RedisCRC64(std::span<const char>(reinterpret_cast<const char*>(binary.data()),
                                                             binary.size())));
}