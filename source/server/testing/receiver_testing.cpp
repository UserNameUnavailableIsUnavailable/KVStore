#include <gtest/gtest.h>

#include <resp/receiver.hpp>
#include <nbio/utility/buffer.hpp>
#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

TEST(RESPReceiving, EmptyBuffersReuseTheirOriginalStorage) {
    nbio::utility::Buffer buffer(64, 64);
    char* const storage = buffer.writable_span().data();
    const std::string bytes(64, 'a');
    for (int iteration = 0; iteration < 1000; ++iteration) {
        ASSERT_TRUE(RESP::detail::PrepareReceiveBuffer(buffer));
        ASSERT_EQ(buffer.writable_size(), 64U);
        EXPECT_EQ(buffer.writable_span().data(), storage);
        ASSERT_TRUE(buffer.write(bytes.data(), bytes.size()));
        buffer.consume(bytes.size());
        EXPECT_EQ(buffer.capacity(), 64U);
    }
}

TEST(RESPReceiving, CompactionPreservesAnIncompleteBinaryCommand) {
    const std::string value("a\0b\r\nc", 6);
    const std::string command = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\n" + value + "\r\n";
    nbio::utility::Buffer buffer(64, 64);
    char* const storage = buffer.writable_span().data();
    const std::string prefix(48, 'x');
    ASSERT_TRUE(buffer.write(prefix.data(), prefix.size()));
    ASSERT_TRUE(buffer.write(command.data(), 16));
    buffer.consume(prefix.size());

    std::vector<std::string_view> words;
    std::size_t size = 0;
    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kNeedInput);
    ASSERT_TRUE(RESP::detail::PrepareReceiveBuffer(buffer));
    EXPECT_EQ(buffer.readable_span().data(), storage);
    EXPECT_EQ(buffer.capacity(), 64U);
    EXPECT_EQ(buffer.string_view(), std::string_view(command).substr(0, 16));
    ASSERT_TRUE(buffer.write(command.data() + 16, command.size() - 16));
    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kComplete);
    ASSERT_EQ(words.size(), 3U);
    EXPECT_EQ(words[2], value);
    EXPECT_EQ(size, command.size());
}

TEST(RESPReceiving, FullUnreadCommandsCanStillGrow) {
    const std::string value(80U * 1024U, 'v');
    const std::string command = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$" + std::to_string(value.size()) +
                                "\r\n" + value + "\r\n";
    nbio::utility::Buffer buffer(64U * 1024U, 16U * 1024U * 1024U);
    const std::size_t initial = buffer.capacity();
    ASSERT_TRUE(buffer.write(command.data(), initial));
    std::vector<std::string_view> words;
    std::size_t size = 0;
    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kNeedInput);
    ASSERT_TRUE(RESP::detail::PrepareReceiveBuffer(buffer));
    EXPECT_GT(buffer.capacity(), initial);
    ASSERT_TRUE(buffer.write(command.data() + initial, command.size() - initial));
    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kComplete);
    ASSERT_EQ(words.size(), 3U);
    EXPECT_EQ(words[2], value);
}

TEST(RESPReceiving, FullUnreadBuffersRespectTheCapacityLimit) {
    nbio::utility::Buffer buffer(64, 64);
    const std::string bytes(64, 'a');
    ASSERT_TRUE(buffer.write(bytes.data(), bytes.size()));
    EXPECT_FALSE(RESP::detail::PrepareReceiveBuffer(buffer));
    EXPECT_EQ(buffer.string_view(), bytes);
    EXPECT_EQ(buffer.capacity(), 64U);
}

TEST(RESPReceiving, YieldedDecoderSurvivesBufferReuse) {
    nbio::utility::Buffer buffer(32, 32);
    auto decoder = RESP::Decode(buffer, RESP::Dialect::kMultibulkAndInline);
    const std::string value(128, 'v');
    const std::string command = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$128\r\n" + value + "\r\n";
    std::size_t offset = 0;
    while (decoder.poll() == RESP::DecodeStatus::kNeedInput) {
        ASSERT_LT(offset, command.size());
        ASSERT_TRUE(RESP::detail::PrepareReceiveBuffer(buffer));
        const std::size_t count = std::min(buffer.writable_size(), command.size() - offset);
        ASSERT_TRUE(buffer.write(command.data() + offset, count));
        offset += count;
        EXPECT_EQ(buffer.capacity(), 32U);
    }
    ASSERT_EQ(decoder.result().status, RESP::DecodeStatus::kComplete);
    ASSERT_TRUE(decoder.result().object);
    const auto& array = std::get<RESP::Array>(decoder.result().object->value);
    ASSERT_EQ(array.values.size(), 3U);
    EXPECT_EQ(std::get<RESP::BulkString>(array.values[2].value).value, value);
    EXPECT_EQ(offset, command.size());
}
