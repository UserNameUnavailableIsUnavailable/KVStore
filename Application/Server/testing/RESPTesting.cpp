// The protocol has two dialects. A client that starts a message with a type
// marker is speaking the one every other test in this directory covers; a client
// that just writes a line of words -- redis-cli, redis-benchmark's PING_INLINE --
// is speaking inline commands, and the decoder turns that line into the
// multi-bulk it means. These tests pin both down, because a benchmark that gets
// 'unknown RESP type marker' for PING is a benchmark that cannot run at all.
#include <Application/RESP/RESP.hpp>

#include <Foundation/Core/Buffer.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace
{
RESP::DecodeResult DecodeBytes(std::string_view bytes, RESP::Dialect dialect = RESP::Dialect::kMultibulkAndInline)
{
    Foundation::Core::Buffer buffer(bytes.size() + 16, bytes.size() + 16);
    EXPECT_TRUE(buffer.write(bytes.data(), bytes.size()));

    auto decoder = RESP::Decode(buffer, dialect);
    while (!decoder.done())
    {
        decoder.resume();
    }
    return decoder.result();
}

// The words of a decoded command, which is either the multi-bulk a RESP client
// sent or the array an inline line was turned into: both look the same here on
// purpose.
std::vector<std::string> WordsOf(const RESP::DecodeResult &result)
{
    if (!result.object)
    {
        return {};
    }
    const auto *array = std::get_if<RESP::Array>(&result.object->value);
    EXPECT_NE(array, nullptr);
    if (array == nullptr)
    {
        return {};
    }

    std::vector<std::string> words;
    for (const RESP::Object &element : array->values)
    {
        const auto *bulk = std::get_if<RESP::BulkString>(&element.value);
        EXPECT_NE(bulk, nullptr);
        if (bulk != nullptr)
        {
            words.push_back(bulk->value.value_or(std::string{}));
        }
    }
    return words;
}
} // namespace

TEST(RESPInline, AnInlinePingIsTheMultibulkItStandsFor)
{
    const RESP::DecodeResult result = DecodeBytes("PING\r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"PING"}));
}

TEST(RESPInline, WordsSeparatedByWhitespaceBecomeBulkStrings)
{
    const RESP::DecodeResult result = DecodeBytes("SET  foo\tbar \r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"SET", "foo", "bar"}));
}

TEST(RESPInline, DoubleQuotesHoldTheSpacesOfOneWord)
{
    const RESP::DecodeResult result = DecodeBytes("SET \"foo bar\" baz\r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"SET", "foo bar", "baz"}));
}

TEST(RESPInline, DoubleQuotesTakeTheUsualEscapes)
{
    const RESP::DecodeResult result = DecodeBytes("SET key \"a\\nb\\x41\"\r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"SET", "key", "a\nbA"}));
}

TEST(RESPInline, SingleQuotesOnlyEscapeAQuote)
{
    const RESP::DecodeResult result = DecodeBytes("SET key 'a\\'b\\nc'\r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"SET", "key", "a'b\\nc"}));
}

TEST(RESPInline, AnEmptyQuotedWordIsStillAWord)
{
    const RESP::DecodeResult result = DecodeBytes("SET key \"\"\r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"SET", "key", ""}));
}

TEST(RESPInline, AQuoteThatIsNeverClosedIsAProtocolError)
{
    const RESP::DecodeResult result = DecodeBytes("SET \"foo bar\r\n");

    EXPECT_EQ(result.status, RESP::DecodeStatus::kProtocolError);
    EXPECT_EQ(result.error, "unbalanced quotes in inline command");
}

TEST(RESPInline, TextAfterAClosingQuoteIsAProtocolError)
{
    const RESP::DecodeResult result = DecodeBytes("SET \"foo\"bar\r\n");

    EXPECT_EQ(result.status, RESP::DecodeStatus::kProtocolError);
    EXPECT_EQ(result.error, "unbalanced quotes in inline command");
}

TEST(RESPInline, ALineWithNoWordsCarriesNoCommand)
{
    // A blank line is not a mistake and not a command: the decoder finishes the
    // line and reports nothing, and the connection carries on.
    const RESP::DecodeResult result = DecodeBytes("\r\n");

    EXPECT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_FALSE(result.object.has_value());
}

TEST(RESPInline, TheMultibulkDialectStillDecodes)
{
    const RESP::DecodeResult result = DecodeBytes("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

    ASSERT_EQ(result.status, RESP::DecodeStatus::kComplete);
    EXPECT_EQ(WordsOf(result), (std::vector<std::string>{"SET", "foo", "bar"}));
}

TEST(RESPInline, AStreamThatOnlySpeaksMultibulkRejectsALineOfWords)
{
    // The replication link is written by this program and never carries an
    // inline command, so a word where a type marker belongs is the protocol
    // error it has always been rather than a command that gets applied.
    const RESP::DecodeResult result = DecodeBytes("SET foo bar\r\n", RESP::Dialect::kMultibulkOnly);

    EXPECT_EQ(result.status, RESP::DecodeStatus::kProtocolError);
    EXPECT_EQ(result.error, "unknown RESP type marker");
}

TEST(RESPInline, BothDialectsShareOneStream)
{
    // A pipeline may mix them, and each message has to leave the buffer exactly
    // where the next one starts.
    Foundation::Core::Buffer buffer(128, 128);
    const std::string stream = "PING\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\nSET k v\r\n";
    ASSERT_TRUE(buffer.write(stream.data(), stream.size()));

    std::vector<std::vector<std::string>> commands;
    while (!buffer.is_empty())
    {
        auto decoder = RESP::Decode(buffer, RESP::Dialect::kMultibulkAndInline);
        while (!decoder.done())
        {
            decoder.resume();
        }
        ASSERT_EQ(decoder.status(), RESP::DecodeStatus::kComplete);
        commands.push_back(WordsOf(decoder.result()));
    }

    ASSERT_EQ(commands.size(), 3U);
    EXPECT_EQ(commands[0], (std::vector<std::string>{"PING"}));
    EXPECT_EQ(commands[1], (std::vector<std::string>{"GET", "foo"}));
    EXPECT_EQ(commands[2], (std::vector<std::string>{"SET", "k", "v"}));
}
