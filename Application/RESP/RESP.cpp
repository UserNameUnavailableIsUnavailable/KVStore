#include "RESP.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <concepts>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace RESP
{
namespace
{
bool ParseInt(std::string_view text, std::int64_t &value)
{
    const auto [end, parse_error] = std::from_chars(text.data(), text.data() + text.size(), value);
    return parse_error == std::errc{} && end == text.data() + text.size();
}

DecodeResult CompleteDecode()
{
    return {.status = DecodeStatus::kComplete, .object = std::nullopt, .error = {}};
}

DecodeResult ErrorDecode(std::string error)
{
    return {.status = DecodeStatus::kProtocolError, .object = std::nullopt, .error = std::move(error)};
}

Decoder ReadLine(::Foundation::Core::Buffer &buffer, std::string &line)
{
    for (;;)
    {
        while (buffer.is_empty())
        {
            co_yield DecodeStatus::kNeedInput;
        }
        const char character = buffer.string_view().front();
        buffer.consume(1);
        if (character != '\r')
        {
            line.push_back(character);
            continue;
        }
        while (buffer.is_empty())
        {
            co_yield DecodeStatus::kNeedInput;
        }
        if (buffer.string_view().front() != '\n')
        {
            co_return ErrorDecode("line is missing LF after CR");
        }
        buffer.consume(1);
        co_return CompleteDecode();
    }
}

Decoder ReadBytes(::Foundation::Core::Buffer &buffer, std::size_t size, std::string &bytes)
{
    while (size != 0)
    {
        while (buffer.is_empty())
        {
            co_yield DecodeStatus::kNeedInput;
        }
        const std::size_t count = std::min(size, buffer.readable_size());
        bytes.append(buffer.string_view().data(), count);
        buffer.consume(count);
        size -= count;
    }
    co_return CompleteDecode();
}

// An inline command is the other dialect of the protocol, and the one redis-cli
// and redis-benchmark's PING_INLINE test speak when they are not sending a
// multi-bulk: a line of whitespace separated words, with quotes around a word
// that has a space in it. It means exactly what the multi-bulk that spelled
// those words out as bulk strings means, which is why it becomes one here and
// nothing above this point ever learns the difference.
bool IsInlineSpace(char character)
{
    return character == ' ' || character == '\t' || character == '\r' || character == '\n' || character == '\v' ||
           character == '\f';
}

bool IsTypeMarker(char character)
{
    switch (character)
    {
    case '+':
    case '-':
    case ':':
    case ',':
    case '_':
    case '#':
    case '(':
    case '$':
    case '!':
    case '=':
    case '*':
    case '~':
    case '%':
    case '|':
    case '>':
        return true;
    default:
        return false;
    }
}

int HexDigit(char character)
{
    if (character >= '0' && character <= '9')
        return character - '0';
    if (character >= 'a' && character <= 'f')
        return character - 'a' + 10;
    if (character >= 'A' && character <= 'F')
        return character - 'A' + 10;
    return -1;
}

// Reads one quoted word, `index` on its opening quote, and leaves the index just
// past the closing one. Double quotes take the escapes a configuration line
// would; single quotes take only a quoted quote, the way redis reads them.
bool ReadQuotedWord(std::string_view line, std::size_t &index, std::string &word)
{
    const char quote = line[index];
    ++index;
    while (index < line.size())
    {
        const char character = line[index];
        if (character == '\\' && quote == '"' && index + 1 < line.size())
        {
            const char escape = line[index + 1];
            index += 2;
            switch (escape)
            {
            case 'n':
                word.push_back('\n');
                break;
            case 'r':
                word.push_back('\r');
                break;
            case 't':
                word.push_back('\t');
                break;
            case 'b':
                word.push_back('\b');
                break;
            case 'a':
                word.push_back('\a');
                break;
            case 'x':
            {
                unsigned value = 0;
                std::size_t digits = 0;
                while (digits < 2 && index < line.size() && HexDigit(line[index]) >= 0)
                {
                    value = value * 16U + static_cast<unsigned>(HexDigit(line[index]));
                    ++index;
                    ++digits;
                }
                if (digits == 0)
                {
                    return false;
                }
                word.push_back(static_cast<char>(value));
                break;
            }
            default:
                word.push_back(escape);
                break;
            }
            continue;
        }
        if (character == '\\' && quote == '\'' && index + 1 < line.size() && line[index + 1] == '\'')
        {
            word.push_back('\'');
            index += 2;
            continue;
        }
        if (character == quote)
        {
            ++index;
            return true;
        }
        word.push_back(character);
        ++index;
    }
    // The quote is never closed: the line is not a command, it is a mistake.
    return false;
}

std::optional<std::vector<std::string>> SplitInlineCommand(std::string_view line)
{
    std::vector<std::string> words;
    std::size_t index = 0;
    while (index < line.size())
    {
        while (index < line.size() && IsInlineSpace(line[index]))
        {
            ++index;
        }
        if (index == line.size())
        {
            break;
        }

        std::string word;
        bool ended = false;
        while (!ended && index < line.size())
        {
            const char character = line[index];
            if (character == '"' || character == '\'')
            {
                if (!ReadQuotedWord(line, index, word))
                {
                    return std::nullopt;
                }
                // A word has to end where its quote does: `"a"b` is a client
                // mistake rather than two words glued together.
                if (index < line.size() && !IsInlineSpace(line[index]))
                {
                    return std::nullopt;
                }
                ended = true;
                continue;
            }
            if (IsInlineSpace(character))
            {
                ended = true;
                continue;
            }
            word.push_back(character);
            ++index;
        }
        words.push_back(std::move(word));
    }
    return words;
}

Decoder ParseObject(::Foundation::Core::Buffer &buffer, std::optional<Object> &output, std::size_t depth);

Decoder ParseAggregate(::Foundation::Core::Buffer &buffer, std::optional<Object> &output, std::size_t depth, char marker, std::size_t count)
{
    if (marker == '%' || marker == '|')
    {
        Map values;
        values.values.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
        {
            std::optional<Object> key;
            auto key_parser = ParseObject(buffer, key, depth + 1);
            while (!key_parser.done())
            {
                key_parser.resume();
                if (!key_parser.done())
                    co_yield key_parser.status();
            }
            if (key_parser.result().status == DecodeStatus::kProtocolError)
                co_return key_parser.result();

            std::optional<Object> value;
            auto value_parser = ParseObject(buffer, value, depth + 1);
            while (!value_parser.done())
            {
                value_parser.resume();
                if (!value_parser.done())
                    co_yield value_parser.status();
            }
            if (value_parser.result().status == DecodeStatus::kProtocolError)
                co_return value_parser.result();
            values.values.emplace_back(std::move(*key), std::move(*value));
        }
        output = marker == '%' ? Object{std::move(values)} : Object{Attribute{.values = std::move(values.values)}};
    }
    else
    {
        std::vector<Object> values;
        values.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
        {
            std::optional<Object> element;
            auto parser = ParseObject(buffer, element, depth + 1);
            while (!parser.done())
            {
                parser.resume();
                if (!parser.done())
                    co_yield parser.status();
            }
            if (parser.result().status == DecodeStatus::kProtocolError)
                co_return parser.result();
            values.push_back(std::move(*element));
        }
        if (marker == '*')
        {
            output = RESP::Object(Array{.values = std::move(values)});
        }
        else if (marker == '~')
        {
            output = RESP::Object(Set{.values = std::move(values)});
        }
        else
        {
            output = RESP::Object(Push{.values = std::move(values)});
        }
    }
    co_return CompleteDecode();
}

Decoder ParseObject(::Foundation::Core::Buffer &buffer, std::optional<Object> &output, std::size_t depth)
{
    constexpr std::size_t kMaximumNesting = 128;
    if (depth > kMaximumNesting)
    {
        co_return ErrorDecode("maximum RESP nesting exceeded");
    }
    while (buffer.is_empty())
    {
        co_yield DecodeStatus::kNeedInput;
    }
    const char marker = buffer.string_view().front();
    buffer.consume(1);

    std::string line;
    auto line_reader = ReadLine(buffer, line);
    while (!line_reader.done())
    {
        line_reader.resume();
        if (!line_reader.done())
            co_yield line_reader.status();
    }
    if (line_reader.result().status == DecodeStatus::kProtocolError)
        co_return line_reader.result();

    if (marker == '+')
        output = RESP::Object(SimpleString{std::move(line)});
    else if (marker == '-')
        output = RESP::Object(SimpleError{std::move(line)});
    else if (marker == '(')
        output = RESP::Object(BigNumber{std::move(line)});
    else if (marker == '_')
    {
        if (!line.empty())
            co_return ErrorDecode("null must not have a value");
        output = RESP::Object(Null{});
    }
    else if (marker == '#')
    {
        if (line == "t")
            output = RESP::Object(Boolean{true});
        else if (line == "f")
            output = RESP::Object(Boolean{false});
        else
            co_return ErrorDecode("invalid boolean");
    }
    else if (marker == ':' || marker == ',')
    {
        if (marker == ':')
        {
            std::int64_t value{};
            if (!ParseInt(line, value))
                co_return ErrorDecode("invalid integer");
            output = RESP::Object(Integer{value});
        }
        else
        {
            double value{};
            const auto [end, error] = std::from_chars(line.data(), line.data() + line.size(), value);
            if (error != std::errc{} || end != line.data() + line.size())
                co_return ErrorDecode("invalid double");
            output = RESP::Object(Double{value});
        }
    }
    else if (marker == '$' || marker == '!' || marker == '=')
    {
        std::int64_t length{};
        if (!ParseInt(line, length))
            co_return ErrorDecode("invalid bulk length");
        if (length == -1 && marker == '$')
        {
            output = RESP::Object(BulkString{std::nullopt});
            co_return CompleteDecode();
        }
        if (length < 0)
            co_return ErrorDecode("bulk length cannot be negative");
        std::string payload;
        payload.reserve(static_cast<std::size_t>(length));
        auto payload_reader = ReadBytes(buffer, static_cast<std::size_t>(length), payload);
        while (!payload_reader.done())
        {
            payload_reader.resume();
            if (!payload_reader.done())
                co_yield payload_reader.status();
        }
        if (payload_reader.result().status == DecodeStatus::kProtocolError)
            co_return payload_reader.result();
        std::string terminator;
        auto terminator_reader = ReadBytes(buffer, 2, terminator);
        while (!terminator_reader.done())
        {
            terminator_reader.resume();
            if (!terminator_reader.done())
                co_yield terminator_reader.status();
        }
        if (terminator_reader.result().status == DecodeStatus::kProtocolError)
            co_return terminator_reader.result();
        if (terminator != "\r\n")
            co_return ErrorDecode("bulk payload is missing CRLF");
        if (marker == '$')
            output = RESP::Object(BulkString{std::move(payload)});
        else if (marker == '!')
            output = RESP::Object(BulkError{std::move(payload)});
        else
        {
            if (payload.size() < 4 || payload[3] != ':')
                co_return ErrorDecode("invalid verbatim string");
            output = RESP::Object(VerbatimString{.format = payload.substr(0, 3), .value = payload.substr(4)});
        }
    }
    else if (marker == '*' || marker == '~' || marker == '%' || marker == '|' || marker == '>')
    {
        std::int64_t count{};
        if (!ParseInt(line, count))
            co_return ErrorDecode("invalid aggregate length");
        if (count == -1 && marker != '>')
        {
            output = RESP::Object(Null{});
            co_return CompleteDecode();
        }
        if (count < 0)
            co_return ErrorDecode("aggregate length cannot be negative");
        auto aggregate_parser = ParseAggregate(buffer, output, depth, marker, static_cast<std::size_t>(count));
        while (!aggregate_parser.done())
        {
            aggregate_parser.resume();
            if (!aggregate_parser.done())
                co_yield aggregate_parser.status();
        }
        if (aggregate_parser.result().status == DecodeStatus::kProtocolError)
            co_return aggregate_parser.result();
    }
    else
        co_return ErrorDecode("unknown RESP type marker");

    co_return CompleteDecode();
}

struct EncodeFrame
{
    enum class Type
    {
        kObject,
        kBytes,
        kObjects,
        kMap,
    };

    Type type;
    const Object *object = nullptr;
    const std::vector<Object> *objects = nullptr;
    const std::vector<std::pair<Object, Object>> *map = nullptr;
    std::string bytes;
    std::string_view borrowed_bytes;
    std::size_t offset = 0;
    std::size_t index = 0;
};

EncodeFrame MakeFrame(EncodeFrame::Type type)
{
    EncodeFrame frame{};
    frame.type = type;
    return frame;
}

std::string EncodeLength(char marker, std::size_t length)
{
    return std::string(1, marker) + std::to_string(length) + "\r\n";
}

void PushBytes(std::vector<EncodeFrame> &frames, std::string bytes)
{
    EncodeFrame frame = MakeFrame(EncodeFrame::Type::kBytes);
    frame.bytes = std::move(bytes);
    frames.push_back(std::move(frame));
}

void PushBorrowedBytes(std::vector<EncodeFrame> &frames, std::string_view bytes)
{
    EncodeFrame frame = MakeFrame(EncodeFrame::Type::kBytes);
    frame.borrowed_bytes = bytes;
    frames.push_back(std::move(frame));
}

void PushLine(std::vector<EncodeFrame> &frames, char marker, std::string_view value)
{
    PushBytes(frames, std::string(1, marker) + std::string(value) + "\r\n");
}

void PushBulk(std::vector<EncodeFrame> &frames, char marker, std::string_view value)
{
    PushBytes(frames, "\r\n");
    PushBorrowedBytes(frames, value);
    PushBytes(frames, EncodeLength(marker, value.size()));
}

void PushOwnedBulk(std::vector<EncodeFrame> &frames, char marker, std::string value)
{
    const std::size_t length = value.size();
    PushBytes(frames, "\r\n");
    PushBytes(frames, std::move(value));
    PushBytes(frames, EncodeLength(marker, length));
}

void PushObject(std::vector<EncodeFrame> &frames, const Object &object)
{
    EncodeFrame frame = MakeFrame(EncodeFrame::Type::kObject);
    frame.object = &object;
    frames.push_back(std::move(frame));
}

void ExpandObject(std::vector<EncodeFrame> &frames, const Object &object)
{
    std::visit(
        [&frames](const auto &value) {
            using Type = std::decay_t<decltype(value)>;
            if constexpr (std::same_as<Type, SimpleString>)
            {
                PushLine(frames, '+', value.value);
            }
            else if constexpr (std::same_as<Type, SimpleError>)
            {
                PushLine(frames, '-', value.value);
            }
            else if constexpr (std::same_as<Type, Integer>)
            {
                PushBytes(frames, ":" + std::to_string(value.value) + "\r\n");
            }
            else if constexpr (std::same_as<Type, BulkString>)
            {
                if (value.value)
                {
                    PushBulk(frames, '$', *value.value);
                }
                else
                {
                    PushBytes(frames, "$-1\r\n");
                }
            }
            else if constexpr (std::same_as<Type, Null>)
            {
                PushBytes(frames, "_\r\n");
            }
            else if constexpr (std::same_as<Type, Boolean>)
            {
                PushBytes(frames, value.value ? "#t\r\n" : "#f\r\n");
            }
            else if constexpr (std::same_as<Type, Double>)
            {
                PushBytes(frames, "," + std::to_string(value.value) + "\r\n");
            }
            else if constexpr (std::same_as<Type, BigNumber>)
            {
                PushLine(frames, '(', value.value);
            }
            else if constexpr (std::same_as<Type, BulkError>)
            {
                PushBulk(frames, '!', value.value);
            }
            else if constexpr (std::same_as<Type, VerbatimString>)
            {
                PushOwnedBulk(frames, '=', value.format + ":" + value.value);
            }
            else if constexpr (std::same_as<Type, Array>)
            {
                EncodeFrame frame = MakeFrame(EncodeFrame::Type::kObjects);
                frame.objects = &value.values;
                frames.push_back(std::move(frame));
                PushBytes(frames, EncodeLength('*', value.values.size()));
            }
            else if constexpr (std::same_as<Type, Set>)
            {
                EncodeFrame frame = MakeFrame(EncodeFrame::Type::kObjects);
                frame.objects = &value.values;
                frames.push_back(std::move(frame));
                PushBytes(frames, EncodeLength('~', value.values.size()));
            }
            else if constexpr (std::same_as<Type, Map>)
            {
                EncodeFrame frame = MakeFrame(EncodeFrame::Type::kMap);
                frame.map = &value.values;
                frames.push_back(std::move(frame));
                PushBytes(frames, EncodeLength('%', value.values.size()));
            }
            else if constexpr (std::same_as<Type, Attribute>)
            {
                EncodeFrame frame = MakeFrame(EncodeFrame::Type::kMap);
                frame.map = &value.values;
                frames.push_back(std::move(frame));
                PushBytes(frames, EncodeLength('|', value.values.size()));
            }
            else if constexpr (std::same_as<Type, Push>)
            {
                EncodeFrame frame = MakeFrame(EncodeFrame::Type::kObjects);
                frame.objects = &value.values;
                frames.push_back(std::move(frame));
                PushBytes(frames, EncodeLength('>', value.values.size()));
            }
        },
        object.value);
}

// The same bytes, written straight into a buffer.
//
// A reply is a handful of bytes that are on their way to a socket, and the way
// above builds four things to get them there: a string for every number, a
// string for every length, a list of those strings, and a coroutine frame to
// yield out of when the buffer is full. Each of those is an allocation, and the
// server answers a million replies a second, so the allocator was 20% of the
// profile with the reply path inside it.
//
// Nothing below allocates. A short line is spelled out in one piece on the
// stack; a long one is copied once, in the order it goes out. The buffer grows
// instead of yielding, because a batch has no other way out than the buffer it
// is being written into.
constexpr std::size_t kMaximumEncodeDepth = 128;

// `marker`, then `text`, then CRLF.
bool AppendLine(::Foundation::Core::Buffer &buffer, char marker, std::string_view text)
{
    char line[64];
    if (text.size() + 3U <= sizeof(line))
    {
        line[0] = marker;
        std::memcpy(line + 1, text.data(), text.size());
        line[text.size() + 1] = '\r';
        line[text.size() + 2] = '\n';
        return buffer.write(line, text.size() + 3U);
    }
    return buffer.write(&marker, 1) && buffer.write(text.data(), text.size()) && buffer.write("\r\n", 2);
}

// `marker`, then a number, then CRLF: the header of every typed reply and the
// length of everything that is sent in bulk.
bool AppendNumberedLine(::Foundation::Core::Buffer &buffer, char marker, std::int64_t number)
{
    char line[32];
    line[0] = marker;
    const auto [end, error] = std::to_chars(line + 1, line + sizeof(line) - 2, number);
    if (error != std::errc{})
    {
        return false;
    }
    *end = '\r';
    *(end + 1) = '\n';
    return buffer.write(line, static_cast<std::size_t>(end - line) + 2U);
}

// `marker`, the length of the payload as a decimal number, CRLF, the payload,
// CRLF.
bool AppendBulk(::Foundation::Core::Buffer &buffer, char marker, std::string_view payload)
{
    return AppendNumberedLine(buffer, marker, static_cast<std::int64_t>(payload.size())) &&
           buffer.write(payload.data(), payload.size()) && buffer.write("\r\n", 2);
}

bool AppendValue(::Foundation::Core::Buffer &buffer, const Object &object, std::size_t depth);

bool AppendValues(::Foundation::Core::Buffer &buffer, const std::vector<Object> &values, std::size_t depth)
{
    for (const Object &value : values)
    {
        if (!AppendValue(buffer, value, depth))
        {
            return false;
        }
    }
    return true;
}

bool AppendPairs(::Foundation::Core::Buffer &buffer, const std::vector<std::pair<Object, Object>> &values, std::size_t depth)
{
    for (const auto &[key, value] : values)
    {
        if (!AppendValue(buffer, key, depth) || !AppendValue(buffer, value, depth))
        {
            return false;
        }
    }
    return true;
}

bool AppendValue(::Foundation::Core::Buffer &buffer, const Object &object, std::size_t depth)
{
    if (depth > kMaximumEncodeDepth)
    {
        return false;
    }

    return std::visit(
        [&buffer, depth](const auto &value) {
            using Type = std::decay_t<decltype(value)>;
            if constexpr (std::same_as<Type, SimpleString>)
            {
                return AppendLine(buffer, '+', value.value);
            }
            else if constexpr (std::same_as<Type, SimpleError>)
            {
                return AppendLine(buffer, '-', value.value);
            }
            else if constexpr (std::same_as<Type, Integer>)
            {
                return AppendNumberedLine(buffer, ':', value.value);
            }
            else if constexpr (std::same_as<Type, BulkString>)
            {
                return value.value ? AppendBulk(buffer, '$', *value.value) : AppendLine(buffer, '$', "-1");
            }
            else if constexpr (std::same_as<Type, Null>)
            {
                return AppendLine(buffer, '_', {});
            }
            else if constexpr (std::same_as<Type, Boolean>)
            {
                return AppendLine(buffer, '#', value.value ? "t" : "f");
            }
            else if constexpr (std::same_as<Type, Double>)
            {
                const std::string text = std::to_string(value.value);
                return AppendLine(buffer, ',', text);
            }
            else if constexpr (std::same_as<Type, BigNumber>)
            {
                return AppendLine(buffer, '(', value.value);
            }
            else if constexpr (std::same_as<Type, BulkError>)
            {
                return AppendBulk(buffer, '!', value.value);
            }
            else if constexpr (std::same_as<Type, VerbatimString>)
            {
                // `format:value` without joining them into a string first: the
                // length is known before either part is written.
                const std::size_t length = value.format.size() + 1U + value.value.size();
                return AppendNumberedLine(buffer, '=', static_cast<std::int64_t>(length)) &&
                       buffer.write(value.format.data(), value.format.size()) && buffer.write(":", 1) &&
                       buffer.write(value.value.data(), value.value.size()) && buffer.write("\r\n", 2);
            }
            else if constexpr (std::same_as<Type, Array> || std::same_as<Type, Set> || std::same_as<Type, Push>)
            {
                constexpr char marker = std::same_as<Type, Array> ? '*' : (std::same_as<Type, Set> ? '~' : '>');
                return AppendNumberedLine(buffer, marker, static_cast<std::int64_t>(value.values.size())) &&
                       AppendValues(buffer, value.values, depth + 1);
            }
            else
            {
                constexpr char marker = std::same_as<Type, Map> ? '%' : '|';
                return AppendNumberedLine(buffer, marker, static_cast<std::int64_t>(value.values.size())) &&
                       AppendPairs(buffer, value.values, depth + 1);
            }
        },
        object.value);
}
} // namespace

Decoder Decode(::Foundation::Core::Buffer &buffer, Dialect dialect)
{
    if (dialect == Dialect::kMultibulkAndInline)
    {
        // Nothing can be said about the message until its first byte is here:
        // that byte is what says which dialect it is written in.
        while (buffer.is_empty())
        {
            co_yield DecodeStatus::kNeedInput;
        }
        if (!IsTypeMarker(buffer.string_view().front()))
        {
            // A line that starts with something other than a type marker is an
            // inline command. Its words become the bulk strings of the
            // multi-bulk it stands for, so the server answers it with the code
            // that answers every other command.
            std::string line;
            auto inline_reader = ReadLine(buffer, line);
            while (!inline_reader.done())
            {
                inline_reader.resume();
                if (!inline_reader.done())
                    co_yield inline_reader.status();
            }
            if (inline_reader.result().status == DecodeStatus::kProtocolError)
                co_return inline_reader.result();

            const std::optional<std::vector<std::string>> words = SplitInlineCommand(line);
            if (!words)
            {
                co_return ErrorDecode("unbalanced quotes in inline command");
            }
            std::optional<Object> command;
            if (!words->empty())
            {
                std::vector<Object> values;
                values.reserve(words->size());
                for (const std::string &word : *words)
                {
                    values.push_back(RESP::Object(BulkString{word}));
                }
                command = RESP::Object(Array{.values = std::move(values)});
            }
            // A line with no command on it is not an error and gets no answer:
            // the line is complete and there is nothing in it, which is what the
            // client that sent a blank line is owed.
            co_return DecodeResult{.status = DecodeStatus::kComplete, .object = std::move(command), .error = {}};
        }
    }

    std::optional<Object> object;
    auto parser = ParseObject(buffer, object, 0);
    while (!parser.done())
    {
        parser.resume();
        if (!parser.done())
            co_yield parser.status();
    }
    const DecodeResult &parsed = parser.result();
    if (parsed.status == DecodeStatus::kProtocolError)
    {
        co_return parsed;
    }
    co_return DecodeResult{.status = DecodeStatus::kComplete, .object = std::move(object), .error = {}};
}

Encoder Encoder::promise_type::get_return_object() noexcept
{
    return Encoder{std::coroutine_handle<promise_type>::from_promise(*this)};
}

std::suspend_always Encoder::promise_type::initial_suspend() noexcept
{
    return {};
}

std::suspend_always Encoder::promise_type::final_suspend() noexcept
{
    return {};
}

std::suspend_always Encoder::promise_type::yield_value(EncodeStatus status) noexcept
{
    status_ = status;
    return {};
}

void Encoder::promise_type::return_void() noexcept
{
    status_ = EncodeStatus::kComplete;
}

void Encoder::promise_type::unhandled_exception() noexcept
{
    std::terminate();
}

Encoder::Encoder(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle)
{
}

Encoder::Encoder(Encoder &&other) noexcept : handle_(std::exchange(other.handle_, {}))
{
}

Encoder &Encoder::operator=(Encoder &&other) noexcept
{
    if (this != &other)
    {
        if (handle_)
        {
            handle_.destroy();
        }
        handle_ = std::exchange(other.handle_, {});
    }
    return *this;
}

Encoder::~Encoder() noexcept
{
    if (handle_)
    {
        handle_.destroy();
    }
}

bool Encoder::done() const noexcept
{
    return !handle_ || handle_.done();
}

EncodeStatus Encoder::poll() noexcept
{
    resume();
    return status();
}

void Encoder::resume() noexcept
{
    if (!done())
    {
        handle_.resume();
    }
}

EncodeStatus Encoder::status() const noexcept
{
    assert(handle_);
    return handle_.promise().status_;
}

Decoder Decoder::promise_type::get_return_object() noexcept
{
    return Decoder{std::coroutine_handle<promise_type>::from_promise(*this)};
}

std::suspend_always Decoder::promise_type::initial_suspend() noexcept
{
    return {};
}

std::suspend_always Decoder::promise_type::final_suspend() noexcept
{
    return {};
}

std::suspend_always Decoder::promise_type::yield_value(DecodeStatus status) noexcept
{
    status_ = status;
    return {};
}

void Decoder::promise_type::return_value(DecodeResult result) noexcept
{
    status_ = result.status;
    result_ = std::move(result);
}

void Decoder::promise_type::unhandled_exception() noexcept
{
    std::terminate();
}

Decoder::Decoder(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle)
{
}

Decoder::Decoder(Decoder &&other) noexcept : handle_(std::exchange(other.handle_, {}))
{
}

Decoder &Decoder::operator=(Decoder &&other) noexcept
{
    if (this != &other)
    {
        if (handle_)
        {
            handle_.destroy();
        }
        handle_ = std::exchange(other.handle_, {});
    }
    return *this;
}

Decoder::~Decoder() noexcept
{
    if (handle_)
    {
        handle_.destroy();
    }
}

bool Decoder::done() const noexcept
{
    return !handle_ || handle_.done();
}

DecodeStatus Decoder::poll()
{
    resume();
    return status();
}

void Decoder::resume()
{
    if (status() == DecodeStatus::kProtocolError)
    {
        throw std::logic_error("cannot resume a decoder after a protocol error");
    }
    if (!done())
    {
        handle_.resume();
    }
}

DecodeStatus Decoder::status() const noexcept
{
    assert(handle_);
    return handle_.promise().status_;
}

const DecodeResult &Decoder::result() const noexcept
{
    assert(handle_ && handle_.done());
    return handle_.promise().result_;
}

DecodeResult &Decoder::result() noexcept
{
    assert(handle_ && handle_.done());
    return handle_.promise().result_;
}

Encoder Encode(const Object &object, ::Foundation::Core::Buffer &buffer)
{
    std::vector<EncodeFrame> frames;
    frames.reserve(16);
    PushObject(frames, object);

    while (!frames.empty())
    {
        EncodeFrame &frame = frames.back();
        if (frame.type == EncodeFrame::Type::kObject)
        {
            const Object *current = frame.object;
            frames.pop_back();
            ExpandObject(frames, *current);
            continue;
        }
        if (frame.type == EncodeFrame::Type::kObjects)
        {
            if (frame.index == frame.objects->size())
            {
                frames.pop_back();
                continue;
            }
            const Object *current = &(*frame.objects)[frame.index++];
            PushObject(frames, *current);
            continue;
        }
        if (frame.type == EncodeFrame::Type::kMap)
        {
            if (frame.index == frame.map->size() * 2)
            {
                frames.pop_back();
                continue;
            }
            const auto &entry = (*frame.map)[frame.index / 2];
            const Object *current = frame.index++ % 2 == 0 ? &entry.first : &entry.second;
            PushObject(frames, *current);
            continue;
        }

        const std::string_view bytes = frame.bytes.empty() ? frame.borrowed_bytes : std::string_view(frame.bytes);
        if (frame.offset == bytes.size())
        {
            frames.pop_back();
            continue;
        }
        if (buffer.write(bytes.data() + frame.offset, bytes.size() - frame.offset))
        {
            frames.pop_back();
            continue;
        }

        std::size_t writable = buffer.writable_size();
        if (writable == 0 && buffer.readable_size() == 0)
        {
            buffer.clear();
            writable = buffer.writable_size();
        }
        if (writable != 0)
        {
            const std::size_t count = std::min(writable, bytes.size() - frame.offset);
            const bool appended = buffer.write(bytes.data() + frame.offset, count);
            assert(appended);
            frame.offset += count;
            continue;
        }
        co_yield EncodeStatus::kNeedFlush;
    }
}

bool AppendObject(const Object &object, ::Foundation::Core::Buffer &buffer)
{
    return AppendValue(buffer, object, 0);
}

namespace
{
// The number that follows a type marker: how many elements an array has, or how
// long a bulk string is. Both are written the same way, and neither can be
// negative in a command -- a `-1` length is a bulk string that is not there,
// which is a reply's shape and not a command's.
//
// The number is read without being copied, and a number too long to be a length
// is not a length: refusing it here hands the bytes to the decoder, which says
// what is wrong with them.
ScanStatus ScanNumber(std::string_view bytes, std::size_t &index, std::int64_t &number)
{
    const std::size_t digits = index;
    while (index < bytes.size() && bytes[index] >= '0' && bytes[index] <= '9')
    {
        const std::int64_t digit = bytes[index] - '0';
        if (number > (std::numeric_limits<std::int64_t>::max() - digit) / 10)
        {
            return ScanStatus::kNotACommand;
        }
        number = number * 10 + digit;
        ++index;
    }

    if (index == digits)
    {
        // A byte that is not a digit says these bytes are not a command; no byte
        // at all says the number has not arrived yet.
        return index == bytes.size() ? ScanStatus::kNeedInput : ScanStatus::kNotACommand;
    }
    if (bytes.size() - index < 2U)
    {
        return ScanStatus::kNeedInput;
    }
    if (bytes[index] != '\r' || bytes[index + 1] != '\n')
    {
        return ScanStatus::kNotACommand;
    }
    index += 2U;
    return ScanStatus::kComplete;
}
} // namespace

ScanStatus ScanCommand(const ::Foundation::Core::Buffer &buffer, std::vector<std::string_view> &words, std::size_t &size)
{
    const std::string_view bytes = buffer.string_view();
    words.clear();
    size = 0;

    std::size_t index = 0;
    if (index == bytes.size())
    {
        return ScanStatus::kNeedInput;
    }
    if (bytes[index] != '*')
    {
        return ScanStatus::kNotACommand;
    }
    ++index;

    std::int64_t count = 0;
    const ScanStatus count_status = ScanNumber(bytes, index, count);
    if (count_status != ScanStatus::kComplete)
    {
        return count_status;
    }

    for (std::int64_t element = 0; element < count; ++element)
    {
        if (index == bytes.size())
        {
            return ScanStatus::kNeedInput;
        }
        if (bytes[index] != '$')
        {
            return ScanStatus::kNotACommand;
        }
        ++index;

        std::int64_t length = 0;
        const ScanStatus length_status = ScanNumber(bytes, index, length);
        if (length_status != ScanStatus::kComplete)
        {
            return length_status;
        }
        const std::size_t payload = static_cast<std::size_t>(length);
        // The payload and the CRLF that ends it have to be here, whole, before
        // a word can be pointed at: half a word is not a word.
        if (bytes.size() - index < payload + 2U)
        {
            return ScanStatus::kNeedInput;
        }
        if (bytes[index + payload] != '\r' || bytes[index + payload + 1] != '\n')
        {
            return ScanStatus::kNotACommand;
        }
        words.emplace_back(bytes.data() + index, payload);
        index += payload + 2U;
    }

    // The command is over where its last element ends: a multi-bulk carries
    // nothing after its elements, unlike the inline line, whose CRLF is the
    // decoder's to read.
    size = index;
    return ScanStatus::kComplete;
}
} // namespace RESP
