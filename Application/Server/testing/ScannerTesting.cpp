// A command is read where it lies: the words of it are views into the receive
// buffer, and nothing is taken out of the buffer until the server has run the
// command and is ready for the next one. That is the whole of the fast path, and
// it has two ways to be wrong that no benchmark would attribute correctly:
//
//   - It can say a command is complete when it is not, or incomplete when it is,
//     which leaves the server waiting for bytes that will never come. The second
//     of those is subtle: a multi-bulk carries nothing after its last element, so
//     a scan that waits for a trailing CRLF waits forever, and a client that
//     pipelines then sees every answer one command late.
//   - It can get a length wrong, which shifts every command after it.
//
// These tests pin both down, on the bytes a client actually writes.
#include <Application/RESP/RESP.hpp>

#include <Foundation/Core/Buffer.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace
{
// A command as a client writes it.
std::string Command(const std::vector<std::string> &words)
{
    std::string bytes = "*" + std::to_string(words.size()) + "\r\n";
    for (const std::string &word : words)
    {
        bytes += "$" + std::to_string(word.size()) + "\r\n" + word + "\r\n";
    }
    return bytes;
}

Foundation::Core::Buffer BufferOf(std::string_view bytes)
{
    Foundation::Core::Buffer buffer(bytes.size() + 16, bytes.size() + 16);
    EXPECT_TRUE(buffer.write(bytes.data(), bytes.size()));
    return buffer;
}

// Every command in the buffer, read the way a connection reads them: scan, take
// the words, consume exactly what the scan said, scan again.
std::vector<std::vector<std::string>> CommandsIn(std::string_view bytes)
{
    Foundation::Core::Buffer buffer = BufferOf(bytes);
    std::vector<std::string_view> words;
    std::size_t size = 0;
    std::vector<std::vector<std::string>> commands;

    while (!buffer.is_empty())
    {
        const RESP::ScanStatus status = RESP::ScanCommand(buffer, words, size);
        EXPECT_EQ(status, RESP::ScanStatus::kComplete);
        EXPECT_GT(size, 0U);
        if (status != RESP::ScanStatus::kComplete || size == 0)
        {
            // Nothing to consume and no command: stopping here is what keeps a
            // failure in the assertion above from becoming a loop that never
            // ends.
            return commands;
        }
        std::vector<std::string> command;
        command.reserve(words.size());
        for (std::string_view word : words)
        {
            command.emplace_back(word);
        }
        commands.push_back(std::move(command));
        buffer.consume(size);
    }
    return commands;
}
} // namespace

// The common case, and the one a benchmark lives on: one command, exactly the
// bytes of it in the buffer, and nothing else behind it. A scanner that expects
// anything after the last element answers nothing at all here.
TEST(RESPScanning, ACommandWithNothingBehindItIsComplete)
{
    const std::string bytes = Command({"SET", "key", "value"});
    Foundation::Core::Buffer buffer = BufferOf(bytes);
    std::vector<std::string_view> words;
    std::size_t size = 0;

    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kComplete);
    EXPECT_EQ(size, bytes.size());
    ASSERT_EQ(words.size(), 3U);
    EXPECT_EQ(words[0], "SET");
    EXPECT_EQ(words[1], "key");
    EXPECT_EQ(words[2], "value");
}

// The words are views into the bytes that were read: that is what makes the fast
// path cost one pass and no allocation, so it is worth a test that says so.
TEST(RESPScanning, TheWordsAreViewsIntoTheBuffer)
{
    const std::string bytes = Command({"GET", "key"});
    Foundation::Core::Buffer buffer = BufferOf(bytes);
    std::vector<std::string_view> words;
    std::size_t size = 0;

    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kComplete);
    const std::string_view held = buffer.string_view();
    for (std::string_view word : words)
    {
        EXPECT_GE(word.data(), held.data());
        EXPECT_LE(word.data() + word.size(), held.data() + held.size());
    }
}

// A pipeline is read one command at a time, each consume leaving the buffer at
// the first byte of the next.
TEST(RESPScanning, APipelineIsReadOneCommandAtATime)
{
    const std::vector<std::vector<std::string>> expected = {
        {"SET", "a", "1"}, {"GET", "a"}, {"DEL", "a"}, {"PING"}, {"SET", "b", ""},
    };
    std::string bytes;
    for (const auto &words : expected)
    {
        bytes += Command(words);
    }
    EXPECT_EQ(CommandsIn(bytes), expected);
}

// A command that arrives in pieces is incomplete until the last byte of it is
// here -- not one byte later, and not one byte earlier.
TEST(RESPScanning, ACommandInPiecesIsCompleteOnItsLastByte)
{
    const std::string bytes = Command({"SET", "key", "value"});
    std::vector<std::string_view> words;
    std::size_t size = 0;

    for (std::size_t length = 0; length < bytes.size(); ++length)
    {
        Foundation::Core::Buffer buffer = BufferOf(std::string_view(bytes).substr(0, length));
        EXPECT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kNeedInput)
            << "a command of " << length << " of " << bytes.size() << " bytes";
    }

    Foundation::Core::Buffer whole = BufferOf(bytes);
    EXPECT_EQ(RESP::ScanCommand(whole, words, size), RESP::ScanStatus::kComplete);
    EXPECT_EQ(size, bytes.size());
}

// An empty argument, and an argument that holds the CRLF of the protocol: the
// length is what says where a word ends, so a payload is allowed to hold
// anything at all.
TEST(RESPScanning, AWordIsAsLongAsItsLengthSaysAndNoLonger)
{
    const std::string bytes = Command({"SET", "key", "line\r\nbreak"});
    const std::vector<std::vector<std::string>> expected = {{"SET", "key", "line\r\nbreak"}};
    EXPECT_EQ(CommandsIn(bytes), expected);
}

// No words at all is still a command: the array is there and the client is owed
// an answer for it, which is what the server says about a command that named
// nothing.
TEST(RESPScanning, AnEmptyCommandIsACommand)
{
    Foundation::Core::Buffer buffer = BufferOf("*0\r\n");
    std::vector<std::string_view> words;
    std::size_t size = 0;

    ASSERT_EQ(RESP::ScanCommand(buffer, words, size), RESP::ScanStatus::kComplete);
    EXPECT_TRUE(words.empty());
    EXPECT_EQ(size, 4U);
}

// The shapes that are not a command read where they lie belong to the decoder,
// which is the reader that knows what they mean. They are handed over -- never
// half-consumed -- so it sees them from the first byte.
TEST(RESPScanning, WhatIsNotACommandInPlaceIsLeftForTheDecoder)
{
    const std::vector<std::string> others = {
        "",                                                  // nothing at all
        "PING\r\n",                                          // the inline dialect
        "*1\r\n+PING\r\n",                                   // a simple string where a bulk belongs
        "*1\r\n:1\r\n",                                      // an integer where a bulk belongs
        "*2\r\n$3\r\nSET\r\n$-1\r\n",                        // a bulk string that is not there
        "*1\r\n$9223372036854775807\r\nx\r\n",               // a length too long to be one
        "*1\r\n$5\r\nvalue\r\r",                             // a payload with no CRLF after it
        "*1\r\n$1",                                          // a length with no number
    };

    for (const std::string &bytes : others)
    {
        Foundation::Core::Buffer buffer = BufferOf(bytes);
        std::vector<std::string_view> words;
        std::size_t size = 0;
        const RESP::ScanStatus status = RESP::ScanCommand(buffer, words, size);
        EXPECT_NE(status, RESP::ScanStatus::kComplete) << "scanned " << bytes;
        // Whatever it decided, it decided on bytes it did not touch: handing them
        // to the decoder means handing it all of them.
        EXPECT_EQ(std::string_view(buffer.string_view()), bytes);
    }
}
