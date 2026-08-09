#include "Common/Protocol.hpp"

#include <charconv>
#include <cctype>
#include <optional>
#include <string>

namespace
{
struct Line
{
    std::string_view value;
    std::size_t next = 0;
};

enum class ValueStatus
{
    kComplete,
    kIncomplete,
    kError,
};

std::optional<Line> ReadLine(std::string_view input, std::size_t offset)
{
    const std::size_t end = input.find("\r\n", offset);
    if (end == std::string_view::npos)
    {
        return std::nullopt;
    }
    return Line {.value = input.substr(offset, end - offset), .next = end + 2};
}

bool ParseSize(std::string_view text, std::size_t& value)
{
    if (text.empty())
    {
        return false;
    }
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    return error == std::errc {} && end == text.data() + text.size();
}

void AppendBulk(std::pmr::string& output, std::string_view value)
{
    output += '$';
    output += std::to_string(value.size());
    output += "\r\n";
    output.append(value.data(), value.size());
    output += "\r\n";
}

void EncodeResult(std::pmr::string& output, const KV::Result& result)
{
    switch (result.type)
    {
    case KV::ResultType::kSimpleString:
        output += '+';
        output += result.value;
        output += "\r\n";
        return;
    case KV::ResultType::kError:
        output += "-ERR ";
        output += result.value;
        output += "\r\n";
        return;
    case KV::ResultType::kBulkString:
        AppendBulk(output, result.value);
        return;
    case KV::ResultType::kArray:
        output += '*';
        output += std::to_string(result.elements.size());
        output += "\r\n";
        for (const KV::Result& element : result.elements)
        {
            EncodeResult(output, element);
        }
        return;
    }
}

ValueStatus DecodeResult(std::string_view input, std::size_t& cursor,
    KV::Result& result, std::pmr::memory_resource* resource, std::pmr::string& error)
{
    if (cursor == input.size())
    {
        return ValueStatus::kIncomplete;
    }

    const char marker = input[cursor++];
    const auto line = ReadLine(input, cursor);
    if (!line)
    {
        return ValueStatus::kIncomplete;
    }
    cursor = line->next;

    if (marker == '+' || marker == '-')
    {
        result.type = marker == '+' ? KV::ResultType::kSimpleString : KV::ResultType::kError;
        const std::string_view value = marker == '-' && line->value.starts_with("ERR ")
            ? line->value.substr(4)
            : line->value;
        result.value = std::pmr::string(value, resource);
        result.elements = std::pmr::vector<KV::Result>(resource);
        return ValueStatus::kComplete;
    }

    std::size_t size = 0;
    if (!ParseSize(line->value, size))
    {
        error = "invalid RESP length";
        return ValueStatus::kError;
    }

    if (marker == '$')
    {
        if (input.size() - cursor < size + 2)
        {
            return ValueStatus::kIncomplete;
        }
        if (input.substr(cursor + size, 2) != "\r\n")
        {
            error = "missing bulk string terminator";
            return ValueStatus::kError;
        }
        result.type = KV::ResultType::kBulkString;
        result.value = std::pmr::string(input.substr(cursor, size), resource);
        result.elements = std::pmr::vector<KV::Result>(resource);
        cursor += size + 2;
        return ValueStatus::kComplete;
    }

    if (marker != '*')
    {
        error = "unknown RESP marker";
        return ValueStatus::kError;
    }

    result.type = KV::ResultType::kArray;
    result.value = std::pmr::string(resource);
    result.elements = std::pmr::vector<KV::Result>(resource);
    result.elements.reserve(size);
    for (std::size_t index = 0; index < size; ++index)
    {
        KV::Result element {.value = std::pmr::string(resource), .elements = std::pmr::vector<KV::Result>(resource)};
        const ValueStatus status = DecodeResult(input, cursor, element, resource, error);
        if (status != ValueStatus::kComplete)
        {
            return status;
        }
        result.elements.push_back(std::move(element));
    }
    return ValueStatus::kComplete;
}

void NormalizeCommand(std::pmr::string& name)
{
    for (char& character : name)
    {
        character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
    }
}
} // namespace

namespace KV
{
Protocol::Protocol(std::pmr::memory_resource* resource) noexcept :
    resource_(resource)
{
}

std::pmr::string Protocol::EncodeRequest(const Command& command) const
{
    std::pmr::string encoded(resource_);
    encoded += '*';
    encoded += std::to_string(command.arguments.size() + 1);
    encoded += "\r\n";
    AppendBulk(encoded, command.name);
    for (const std::pmr::string& argument : command.arguments)
    {
        AppendBulk(encoded, argument);
    }
    return encoded;
}

RequestDecode Protocol::DecodeRequest(std::string_view input) const
{
    RequestDecode decoded {.error = std::pmr::string(resource_),
        .command = {.name = std::pmr::string(resource_), .arguments = std::pmr::vector<std::pmr::string>(resource_)}};
    if (input.empty())
    {
        return decoded;
    }
    if (input.front() != '*')
    {
        decoded.status = DecodeStatus::kProtocolError;
        decoded.error = "request must be a RESP array";
        return decoded;
    }

    const auto count_line = ReadLine(input, 1);
    if (!count_line)
    {
        return decoded;
    }
    std::size_t count = 0;
    if (!ParseSize(count_line->value, count) || count == 0)
    {
        decoded.status = DecodeStatus::kProtocolError;
        decoded.error = "request array must contain a command";
        return decoded;
    }

    std::size_t cursor = count_line->next;
    std::pmr::vector<std::pmr::string> tokens(resource_);
    tokens.reserve(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        if (cursor == input.size())
        {
            return decoded;
        }
        if (input[cursor++] != '$')
        {
            decoded.status = DecodeStatus::kProtocolError;
            decoded.error = "request array elements must be bulk strings";
            return decoded;
        }
        const auto length_line = ReadLine(input, cursor);
        if (!length_line)
        {
            return decoded;
        }
        std::size_t length = 0;
        if (!ParseSize(length_line->value, length))
        {
            decoded.status = DecodeStatus::kProtocolError;
            decoded.error = "invalid request bulk string length";
            return decoded;
        }
        cursor = length_line->next;
        if (input.size() - cursor < length + 2)
        {
            return decoded;
        }
        if (input.substr(cursor + length, 2) != "\r\n")
        {
            decoded.status = DecodeStatus::kProtocolError;
            decoded.error = "missing request bulk string terminator";
            return decoded;
        }
        tokens.emplace_back(input.substr(cursor, length));
        cursor += length + 2;
    }

    decoded.status = DecodeStatus::kComplete;
    decoded.consumed_bytes = cursor;
    decoded.command.name = std::move(tokens.front());
    NormalizeCommand(decoded.command.name);
    decoded.command.arguments.assign(std::make_move_iterator(tokens.begin() + 1), std::make_move_iterator(tokens.end()));
    return decoded;
}

std::pmr::string Protocol::EncodeResponse(const Result& result) const
{
    std::pmr::string encoded(resource_);
    EncodeResult(encoded, result);
    return encoded;
}

ResponseDecode Protocol::DecodeResponse(std::string_view input) const
{
    ResponseDecode decoded {.error = std::pmr::string(resource_),
        .result = {.value = std::pmr::string(resource_), .elements = std::pmr::vector<Result>(resource_)}};
    std::size_t cursor = 0;
    const ValueStatus status = DecodeResult(input, cursor, decoded.result, resource_, decoded.error);
    if (status == ValueStatus::kIncomplete)
    {
        return decoded;
    }
    if (status == ValueStatus::kError)
    {
        decoded.status = DecodeStatus::kProtocolError;
        return decoded;
    }
    decoded.status = DecodeStatus::kComplete;
    decoded.consumed_bytes = cursor;
    return decoded;
}
} // namespace KV
