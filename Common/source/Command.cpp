#include "Common/Command.hpp"

#include <charconv>
#include <cctype>
#include <optional>

namespace
{
struct LineRead
{
    std::string_view line;
    std::size_t consumed = 0;
};

std::optional<LineRead> ReadLine(std::string_view input, std::size_t start)
{
    if (start >= input.size())
    {
        return std::nullopt;
    }

    const std::size_t lf = input.find('\n', start);
    if (lf == std::string_view::npos)
    {
        return std::nullopt;
    }

    std::size_t line_end = lf;
    if (line_end > start && input[line_end - 1] == '\r')
    {
        --line_end;
    }

    return LineRead {.line = input.substr(start, line_end - start), .consumed = (lf - start) + 1};
}

bool ParseNonNegativeInteger(std::string_view text, std::size_t& out)
{
    if (text.empty())
    {
        return false;
    }

    for (char character : text)
    {
        if (!std::isdigit(static_cast<unsigned char>(character)))
        {
            return false;
        }
    }

    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto [parsed_end, error] = std::from_chars(begin, end, out);
    return error == std::errc {} && parsed_end == end;
}

bool ConsumeLineEnding(std::string_view input, std::size_t& cursor)
{
    if (cursor >= input.size())
    {
        return false;
    }

    if (input[cursor] == '\n')
    {
        ++cursor;
        return true;
    }

    if (input[cursor] == '\r' && cursor + 1 < input.size() && input[cursor + 1] == '\n')
    {
        cursor += 2;
        return true;
    }

    return false;
}

std::string ToUpper(std::string value)
{
    for (char& character : value)
    {
        character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
    }
    return value;
}
} // namespace

namespace KV
{
Command::Command(std::string name, std::vector<std::string> arguments) :
    name_(std::move(name)),
    arguments_(std::move(arguments))
{
}

const std::string& Command::GetName() const
{
    return name_;
}

const std::vector<std::string>& Command::GetArguments() const
{
    return arguments_;
}

std::string Command::Serialize() const
{
    std::string serialized = std::to_string(arguments_.size() + 1) + "\r\n";
    serialized += std::to_string(name_.size()) + "\r\n" + name_ + "\r\n";
    for (const std::string& argument : arguments_)
    {
        serialized += std::to_string(argument.size()) + "\r\n" + argument + "\r\n";
    }
    return serialized;
}

CommandParseResult Command::Deserialize(std::string_view input)
{
    name_.clear();
    arguments_.clear();

    CommandParseResult result;
    const auto count_line = ReadLine(input, 0);
    if (!count_line.has_value())
    {
        return result;
    }

    std::size_t token_count = 0;
    if (!ParseNonNegativeInteger(count_line->line, token_count))
    {
        return {.status = CommandParseStatus::kProtocolError,
            .consumed_bytes = count_line->consumed,
            .error_message = "invalid token count"};
    }
    if (token_count == 0)
    {
        return {.status = CommandParseStatus::kProtocolError,
            .consumed_bytes = count_line->consumed,
            .error_message = "empty request"};
    }

    std::size_t cursor = count_line->consumed;
    std::vector<std::string> tokens;
    tokens.reserve(token_count);
    for (std::size_t index = 0; index < token_count; ++index)
    {
        const auto length_line = ReadLine(input, cursor);
        if (!length_line.has_value())
        {
            return result;
        }

        std::size_t token_length = 0;
        if (!ParseNonNegativeInteger(length_line->line, token_length))
        {
            return {.status = CommandParseStatus::kProtocolError,
                .consumed_bytes = cursor + length_line->consumed,
                .error_message = "invalid token length"};
        }
        cursor += length_line->consumed;

        if (token_length > input.size() - cursor)
        {
            return result;
        }
        tokens.emplace_back(input.substr(cursor, token_length));
        cursor += token_length;

        if (!ConsumeLineEnding(input, cursor))
        {
            if (cursor >= input.size())
            {
                return result;
            }
            return {.status = CommandParseStatus::kProtocolError,
                .consumed_bytes = cursor,
                .error_message = "missing token terminator"};
        }
    }

    name_ = ToUpper(std::move(tokens.front()));
    arguments_.assign(std::make_move_iterator(tokens.begin() + 1), std::make_move_iterator(tokens.end()));
    return {.status = CommandParseStatus::kOk, .consumed_bytes = cursor};
}
} // namespace KV