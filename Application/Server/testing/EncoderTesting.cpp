// Writing a reply is the hottest thing the server does: a benchmark at a
// hundred connections spends more time turning answers into bytes than it does
// answering. So there are two writers for one format -- the coroutine that can
// stop half way through a reply when the writer it feeds has to flush (the AOF
// filling a buffer on its way to the file), and the direct one that has nowhere
// to flush to and grows the buffer instead (the reply batch, which goes to a
// socket and has no other way out).
//
// Two writers of one format is two chances to disagree about it, and a reply
// that differs by one byte is a client that hangs. These tests hold them to the
// same bytes: whatever the streaming one writes, the direct one writes, for
// every reply the protocol has.
#include <Application/RESP/RESP.hpp>

#include <Foundation/Core/Buffer.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace
{
// The bytes the coroutine writer produces, growing the buffer the way the AOF
// does when it has nowhere to flush yet.
std::string Streamed(const RESP::Object &object, std::size_t capacity = 16, std::size_t max_capacity = 1U << 20)
{
    Foundation::Core::Buffer buffer(capacity, max_capacity);
    auto encoder = RESP::Encode(object, buffer);
    while (encoder.poll() == RESP::EncodeStatus::kNeedFlush)
    {
        if (!buffer.reserve(buffer.capacity()))
        {
            return "<will not fit>";
        }
    }
    return std::string(buffer.string_view());
}

// The bytes the direct writer produces, which has to agree with them.
std::string Direct(const RESP::Object &object, std::size_t capacity = 16, std::size_t max_capacity = 1U << 20)
{
    Foundation::Core::Buffer buffer(capacity, max_capacity);
    if (!RESP::AppendObject(object, buffer))
    {
        return "<will not fit>";
    }
    return std::string(buffer.string_view());
}

void ExpectSameBytes(const RESP::Object &object)
{
    EXPECT_EQ(Direct(object), Streamed(object));
}

// A reply of every type the protocol has, and of every shape the server builds:
// the nested ones matter because a writer that recurses and a writer that walks a
// list of pieces are two different ways to get the order of a reply wrong.
std::vector<RESP::Object> EveryReply()
{
    const std::string long_value(4096, 'v');
    return {
        RESP::Object(RESP::SimpleString{.value = "OK"}),
        RESP::Object(RESP::SimpleString{.value = {}}),
        RESP::Object(RESP::SimpleError{.value = "ERR unknown command 'nope'"}),
        RESP::Object(RESP::Integer{.value = 0}),
        RESP::Object(RESP::Integer{.value = -9223372036854775807LL - 1}),
        RESP::Object(RESP::Integer{.value = 9223372036854775807LL}),
        RESP::Object(RESP::BulkString{.value = std::string{"value"}}),
        RESP::Object(RESP::BulkString{.value = long_value}),
        RESP::Object(RESP::BulkString{.value = std::nullopt}),
        RESP::Object(RESP::Null{}),
        RESP::Object(RESP::Boolean{.value = true}),
        RESP::Object(RESP::Boolean{.value = false}),
        RESP::Object(RESP::Double{.value = 3.5}),
        RESP::Object(RESP::BigNumber{.value = "3492890328409238509324850943850943825024385"}),
        RESP::Object(RESP::BulkError{.value = "SYNTAX invalid syntax"}),
        RESP::Object(RESP::VerbatimString{.format = "txt", .value = "Some string"}),
        RESP::Object(RESP::Array{}),
        RESP::Object(RESP::Array{.values = {RESP::Object(RESP::Integer{.value = 1}),
                                             RESP::Object(RESP::BulkString{.value = std::string{"two"}}),
                                             RESP::Object(RESP::Null{})}}),
        RESP::Object(RESP::Set{.values = {RESP::Object(RESP::SimpleString{.value = "a"})}}),
        RESP::Object(RESP::Push{.values = {RESP::Object(RESP::SimpleString{.value = "message"})}}),
        RESP::Object(RESP::Map{.values = {{RESP::Object(RESP::SimpleString{.value = "key"}),
                                           RESP::Object(RESP::Integer{.value = 7})}}}),
        RESP::Object(RESP::Attribute{.values = {{RESP::Object(RESP::SimpleString{.value = "ttl"}),
                                                 RESP::Object(RESP::Integer{.value = 60})}}}),
        // An array of arrays, which is what EXEC answers with.
        RESP::Object(RESP::Array{.values = {
                                     RESP::Object(RESP::Array{.values = {RESP::Object(RESP::BulkString{.value = std::string{"a"}})}}),
                                     RESP::Object(RESP::Array{.values = {RESP::Object(RESP::Integer{.value = 1}),
                                                                         RESP::Object(RESP::Array{})}}),
                                     RESP::Object(RESP::SimpleError{.value = "ERR wrong number of arguments"}),
                                 }}),
    };
}
} // namespace

TEST(RESPEncoding, EveryReplyIsTheSameBytesWrittenDirectlyAsStreamed)
{
    for (const RESP::Object &object : EveryReply())
    {
        ExpectSameBytes(object);
    }
}

// The common reply is a handful of bytes, but a store holds values of any size,
// so the writer has to be right about the ones that do not fit in the buffer it
// was given: it grows, and the bytes that come out are the same either way. A
// writer that grows by moving data would produce the right answer here only by
// accident, which is why this is asked of both.
TEST(RESPEncoding, AReplyLargerThanTheBufferIsTheSameBytesEitherWay)
{
    for (const std::size_t length : {1U, 15U, 16U, 17U, 63U, 64U, 65U, 1024U, 70000U})
    {
        const RESP::Object object(RESP::BulkString{.value = std::string(length, 'x')});
        EXPECT_EQ(Direct(object, 8, 1U << 20), Streamed(object, 8, 1U << 20)) << "bulk of " << length << " bytes";
    }
}

// A reply the buffer cannot hold is refused rather than truncated: half a reply
// is a client waiting for the rest of it forever.
TEST(RESPEncoding, AReplyTooLargeForTheBufferIsRefused)
{
    Foundation::Core::Buffer buffer(16, 64);
    EXPECT_FALSE(RESP::AppendObject(RESP::Object(RESP::BulkString{.value = std::string(1024, 'x')}), buffer));
}

// A reply nested deeper than the encoder will follow is refused, not walked
// until the stack runs out.
TEST(RESPEncoding, AReplyNestedPastTheLimitIsRefused)
{
    auto nested = [](std::size_t depth) {
        RESP::Object object(RESP::SimpleString{.value = "bottom"});
        for (std::size_t level = 0; level < depth; ++level)
        {
            object = RESP::Object(RESP::Array{.values = {std::move(object)}});
        }
        return object;
    };

    Foundation::Core::Buffer roomy(16, 1U << 20);
    EXPECT_TRUE(RESP::AppendObject(nested(100), roomy));
    EXPECT_EQ(Direct(nested(100)), Streamed(nested(100)));

    Foundation::Core::Buffer buffer(16, 1U << 20);
    EXPECT_FALSE(RESP::AppendObject(nested(200), buffer));
}
