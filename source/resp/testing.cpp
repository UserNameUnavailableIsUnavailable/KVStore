#include <gtest/gtest.h>

#include <resp/decoder.hpp>
#include <resp/encoder.hpp>

#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
struct Case {
    resp::Object object;
    std::string wire;
};

std::vector<Case> Cases() {
    using namespace resp;
    return {
        {Object(SimpleString{"OK"}), "+OK\r\n"},
        {Object(SimpleError{"ERR failed"}), "-ERR failed\r\n"},
        {Object(Integer{0}), ":0\r\n"},
        {Object(Integer{std::numeric_limits<std::int64_t>::min()}), ":-9223372036854775808\r\n"},
        {Object(Integer{std::numeric_limits<std::int64_t>::max()}), ":9223372036854775807\r\n"},
        {Object(BulkString{std::nullopt}), "$-1\r\n"},
        {Object(BulkString{std::string{}}), "$0\r\n\r\n"},
        {Object(BulkString{std::string("a\0b\r\nc", 6)}), std::string("$6\r\na\0b\r\nc\r\n", 12)},
        {Object(Null{}), "_\r\n"},
        {Object(Boolean{true}), "#t\r\n"},
        {Object(Boolean{false}), "#f\r\n"},
        {Object(Double{3.5}), ",3.5\r\n"},
        {Object(Double{1.2345678901234567}), ",1.2345678901234567\r\n"},
        {Object(Double{std::numeric_limits<double>::infinity()}), ",inf\r\n"},
        {Object(Double{-std::numeric_limits<double>::infinity()}), ",-inf\r\n"},
        {Object(Double{std::numeric_limits<double>::quiet_NaN()}), ",nan\r\n"},
        {Object(BigNumber{"3492890328409238509324850943850943825024385"}),
         "(3492890328409238509324850943850943825024385\r\n"},
        {Object(BulkError{"ERR"}), "!3\r\nERR\r\n"},
        {Object(VerbatimString{"txt", "hello"}), "=9\r\ntxt:hello\r\n"},
        {Object(Array{}), "*0\r\n"},
        {Object(Set{{Object(Integer{1}), Object(Integer{2})}}), "~2\r\n:1\r\n:2\r\n"},
        {Object(Push{{Object(SimpleString{"message"})}}), ">1\r\n+message\r\n"},
        {Object(Map{{{Object(SimpleString{"key"}), Object(Integer{7})}}}), "%1\r\n+key\r\n:7\r\n"},
        {Object(Attribute{{{Object(SimpleString{"ttl"}), Object(Integer{60})}}}), "|1\r\n+ttl\r\n:60\r\n"},
        {Object(Array{{Object(Array{{Object(Integer{1})}}), Object(Map{}), Object(Null{})}}),
         "*3\r\n*1\r\n:1\r\n%0\r\n_\r\n"},
    };
}

std::string Encode(const resp::Object& object, std::size_t capacity) {
    resp::Encoder encoder(object);
    std::vector<char> buffer(capacity);
    std::string wire;
    for (std::size_t iteration = 0; iteration < 100000; ++iteration) {
        const auto result = encoder.Encode(buffer);
        EXPECT_LE(result.encoded, buffer.size());
        wire.append(buffer.data(), result.encoded);
        if (result.status) {
            EXPECT_EQ(result.status->size(), result.encoded);
            EXPECT_TRUE(encoder.done());
            EXPECT_EQ(encoder.Encode(buffer).encoded, 0U);
            return wire;
        }
        EXPECT_EQ(result.status.error(), resp::make_error_code(resp::EncodeError::kMore));
        if (result.status.error() != resp::make_error_code(resp::EncodeError::kMore)) return "<error>";
        EXPECT_EQ(result.encoded, buffer.size());
    }
    ADD_FAILURE() << "Encoder made no progress";
    return "<error>";
}
} // namespace

TEST(RespGenerator, AllTypesEncodeInArbitrarilySmallBuffers) {
    for (const auto& test : Cases()) {
        for (const std::size_t size : {1U, 2U, 7U, 64U}) {
            SCOPED_TRACE(test.wire);
            EXPECT_EQ(Encode(test.object, size), test.wire);
        }
    }
}

TEST(RespGenerator, AllTypesDecodeAcrossEveryFragmentBoundary) {
    for (const auto& test : Cases()) {
        for (const std::size_t size : {1U, 2U, 7U, 64U}) {
            SCOPED_TRACE(test.wire);
            resp::Decoder decoder;
            std::size_t offset = 0;
            bool complete = false;
            while (offset < test.wire.size()) {
                const std::string fragment = test.wire.substr(offset, size);
                auto result = decoder.Decode(std::span<const char>(fragment));
                ASSERT_GT(result.decoded, 0U);
                ASSERT_LE(result.decoded, fragment.size());
                offset += result.decoded;
                if (result.value) {
                    EXPECT_EQ(offset, test.wire.size());
                    EXPECT_EQ(Encode(*result.value, 3), test.wire);
                    complete = true;
                    break;
                }
                ASSERT_EQ(result.value.error(), resp::DecodeError::kMore);
            }
            EXPECT_TRUE(complete);
        }
    }
}

TEST(RespGenerator, PipelineStopsExactlyAtEachObject) {
    resp::Decoder decoder;
    const std::string wire = "+OK\r\n:7\r\n$3\r\nend\r\n";
    auto remaining = std::span<const char>(wire);
    for (const std::string expected : {"+OK\r\n", ":7\r\n", "$3\r\nend\r\n"}) {
        auto result = decoder.Decode(remaining);
        ASSERT_TRUE(result.value);
        EXPECT_EQ(result.decoded, expected.size());
        EXPECT_EQ(Encode(*result.value, 1), expected);
        remaining = remaining.subspan(result.decoded);
    }
    EXPECT_TRUE(remaining.empty());
    const auto result = decoder.Decode({});
    ASSERT_FALSE(result.value);
    EXPECT_EQ(result.value.error(), resp::DecodeError::kMore);
    EXPECT_EQ(result.decoded, 0U);
}

TEST(RespGenerator, InvalidFramesRemainTerminal) {
    for (const std::string wire : {"?\r\n", ":abc\r\n", ":9223372036854775808\r\n", "#x\r\n",
                                   "_x\r\n", "$-2\r\n", "!-1\r\n", "~-1\r\n", ">-1\r\n",
                                   "$1\r\nxXX", "=4\r\ntxt?\r\n", "(12x\r\n", "+bad\n"}) {
        SCOPED_TRACE(wire);
        resp::Decoder decoder;
        auto result = decoder.Decode(std::span<const char>(wire));
        ASSERT_FALSE(result.value);
        EXPECT_EQ(result.value.error(), resp::DecodeError::kInvalid);
        EXPECT_LE(result.decoded, wire.size());
        result = decoder.Decode(std::span<const char>(wire));
        ASSERT_FALSE(result.value);
        EXPECT_EQ(result.value.error(), resp::DecodeError::kInvalid);
        EXPECT_EQ(result.decoded, 0U);
    }
}

TEST(RespGenerator, EmptyBuffersAndMoveOwnershipPreserveProgress) {
    static_assert(!std::is_copy_constructible_v<resp::Encoder>);
    static_assert(!std::is_copy_constructible_v<resp::Decoder>);
    const resp::Object object(resp::BulkString{std::string("hello")});
    resp::Encoder original(object);
    auto result = original.Encode({});
    ASSERT_FALSE(result.status);
    EXPECT_EQ(result.encoded, 0U);
    char buffer[2];
    result = original.Encode(buffer);
    std::string wire(buffer, result.encoded);
    resp::Encoder moved(std::move(original));
    EXPECT_TRUE(original.done());
    resp::Encoder assigned(object);
    assigned = std::move(moved);
    while (!assigned.done()) {
        result = assigned.Encode(buffer);
        wire.append(buffer, result.encoded);
    }
    EXPECT_EQ(wire, "$5\r\nhello\r\n");

    resp::Decoder decoder;
    EXPECT_EQ(decoder.Decode({}).decoded, 0U);
    const std::string prefix = "$5\r\nhe";
    auto decoded = decoder.Decode(std::span<const char>(prefix));
    ASSERT_FALSE(decoded.value);
    EXPECT_EQ(decoded.value.error(), resp::DecodeError::kMore);
    resp::Decoder moved_decoder(std::move(decoder));
    resp::Decoder assigned_decoder;
    assigned_decoder = std::move(moved_decoder);
    const std::string suffix = "llo\r\n";
    decoded = assigned_decoder.Decode(std::span<const char>(suffix));
    ASSERT_TRUE(decoded.value);
    EXPECT_EQ(decoded.decoded, suffix.size());
    EXPECT_EQ(Encode(*decoded.value, 2), "$5\r\nhello\r\n");
}

TEST(RespGenerator, NestingIsBoundedInBothDirections) {
    resp::Object object(resp::Integer{1});
    std::string wire = ":1\r\n";
    for (std::size_t depth = 0; depth < 150; ++depth) {
        object = resp::Object(resp::Array{{std::move(object)}});
        wire = "*1\r\n" + wire;
    }
    resp::Encoder encoder(object);
    char buffer[4096];
    const auto encoded = encoder.Encode(buffer);
    ASSERT_FALSE(encoded.status);
    EXPECT_EQ(encoded.status.error(), resp::make_error_code(resp::EncodeError::kInternal));
    EXPECT_TRUE(encoder.done());
    resp::Decoder decoder;
    const auto decoded = decoder.Decode(std::span<const char>(wire));
    ASSERT_FALSE(decoded.value);
    EXPECT_EQ(decoded.value.error(), resp::DecodeError::kInvalid);
}