#include "RESP.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <concepts>
#include <iostream>
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
        const std::size_t count = std::min(size, buffer.valid_size());
        bytes.append(buffer.string_view().data(), count);
        buffer.consume(count);
        size -= count;
    }
    co_return CompleteDecode();
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
} // namespace

Decoder Decode(::Foundation::Core::Buffer &buffer)
{
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
        if (buffer.append(bytes.data() + frame.offset, bytes.size() - frame.offset))
        {
            frames.pop_back();
            continue;
        }

        std::size_t writable = buffer.appendable_size();
        if (writable == 0 && buffer.valid_size() == 0)
        {
            buffer.clear();
            writable = buffer.appendable_size();
        }
        if (writable != 0)
        {
            const std::size_t count = std::min(writable, bytes.size() - frame.offset);
            const bool appended = buffer.append(bytes.data() + frame.offset, count);
            assert(appended);
            frame.offset += count;
            continue;
        }
        co_yield EncodeStatus::kNeedFlush;
    }
}
} // namespace RESP
