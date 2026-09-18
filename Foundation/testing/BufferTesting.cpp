// A buffer is a window over one allocation: bytes go in at the end, come out at
// the front, and what has been read but not consumed is still in there. Growing
// it has to move that window along with the bytes, because a window left where
// it was describes a part of the new allocation the bytes were never copied to.
//
// That is not a corner case for a server: a receive buffer holding the first half
// of a command, or a reply batch that has grown to its first allocation and needs
// more, is exactly a buffer that holds bytes past the front of its storage and
// then grows.
#include <Foundation/Core/Buffer.hpp>

#include <gtest/gtest.h>

#include <cstddef>
#include <string>

namespace
{
std::string Fill(std::size_t size, char character)
{
    return std::string(size, character);
}

void Append(Foundation::Core::Buffer &buffer, const std::string &bytes)
{
    ASSERT_TRUE(buffer.write(bytes.data(), bytes.size()));
}
} // namespace

TEST(Buffer, WhatIsWrittenIsWhatIsRead)
{
    Foundation::Core::Buffer buffer(64, 64);
    Append(buffer, "hello");
    EXPECT_EQ(buffer.string_view(), "hello");
    EXPECT_EQ(buffer.readable_size(), 5U);

    buffer.consume(5);
    EXPECT_TRUE(buffer.is_empty());
    EXPECT_EQ(buffer.string_view(), "");

    Append(buffer, "again");
    EXPECT_EQ(buffer.string_view(), "again");
}

// The case the whole class exists for: more arrives than the buffer was given
// room for, and it grows instead of dropping it.
TEST(Buffer, WritingPastWhatItHoldsGrowsTheBuffer)
{
    Foundation::Core::Buffer buffer(8, 4096);
    const std::string bytes = "a line that is longer than the buffer";
    Append(buffer, bytes);
    EXPECT_EQ(buffer.string_view(), bytes);
}

// The regression: bytes that were read but not consumed are not at the front of
// the storage, and a grow has to keep them where the buffer says they are. A
// grow that copies them to the front while leaving the window where it was hands
// the reader whatever happens to be at the old offsets -- here, nothing at all.
TEST(Buffer, AFullBufferThatHoldsBytesGrowsAndKeepsThem)
{
    Foundation::Core::Buffer buffer(16, 4096);
    const std::string held = Fill(16, 'h');
    Append(buffer, held);

    // Read all but the tail of it: what is left is a command that is only half
    // here, which is the state a receive buffer grows in.
    buffer.consume(10);
    EXPECT_EQ(buffer.string_view(), Fill(6, 'h'));
    EXPECT_EQ(buffer.writable_size(), 0U);

    // The rest of it arrives and there is no room: the buffer grows.
    Append(buffer, "rest of it");
    EXPECT_EQ(buffer.string_view(), Fill(6, 'h') + "rest of it");
}

// The same, standing on the ceiling: a buffer that cannot grow any further keeps
// what it holds rather than losing it, and says that the write did not happen.
TEST(Buffer, ABufferAtItsCeilingKeepsWhatItHolds)
{
    Foundation::Core::Buffer buffer(16, 24);
    Append(buffer, Fill(16, 'a'));
    buffer.consume(4);
    EXPECT_EQ(buffer.string_view(), Fill(12, 'a'));

    EXPECT_FALSE(buffer.write(Fill(64, 'b').data(), 64));
    EXPECT_EQ(buffer.string_view(), Fill(12, 'a'));
    Append(buffer, "12345678");
    EXPECT_EQ(buffer.string_view(), Fill(12, 'a') + "12345678");
}
