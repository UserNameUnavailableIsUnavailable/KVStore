#include "Common/Parser.hpp"

#include <charconv>
#include <cctype>
#include <optional>

struct LineRead
{
    std::string_view line;
    std::size_t consumed = 0;
};

static std::optional<LineRead> ReadLine(std::string_view input, std::size_t start)
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

    return LineRead {
        .line = input.substr(start, line_end - start),
        .consumed = (lf - start) + 1,
    };
}

static bool ParseNonNegativeInteger(std::string_view text, std::size_t& out)
{
    if (text.empty())
    {
        return false;
    }

    for (char c : text)
    {
        if (!std::isdigit(static_cast<unsigned char>(c)))
        {
            return false;
        }
    }

    std::size_t value = 0;
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc() || ptr != end)
    {
        return false;
    }

    out = value;
    return true;
}

static bool ConsumeLineEnding(std::string_view input, std::size_t& cursor)
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

    if (input[cursor] == '\r')
    {
        if (cursor + 1 >= input.size())
        {
            return false;
        }
        if (input[cursor + 1] != '\n')
        {
            return false;
        }
        cursor += 2;
        return true;
    }

    return false;
}

static std::string ToUpper(std::string s)
{
    for (char& c : s)
    {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return s;
}

namespace KV
{
ParseResult Parser::Parse(std::string_view input) const
{
    ParseResult result;

    if (input.empty())
    {
        result.status = ParseStatus::kIncomplete;
        return result;
    }

    std::size_t cursor = 0;
    auto count_line = ReadLine(input, cursor);
    if (!count_line.has_value())
    {
        result.status = ParseStatus::kIncomplete;
        return result;
    }

    std::size_t token_count = 0;
    if (!ParseNonNegativeInteger(count_line->line, token_count))
    {
        result.status = ParseStatus::kProtocolError;
        result.consumed_bytes = count_line->consumed;
        result.error_message = "invalid token count";
        return result;
    }

    cursor += count_line->consumed;

    if (token_count == 0)
    {
        result.status = ParseStatus::kProtocolError;
        result.consumed_bytes = cursor;
        result.error_message = "empty request";
        return result;
    }

    std::vector<std::string> tokens;
    tokens.reserve(token_count);

    for (std::size_t i = 0; i < token_count; ++i)
    {
        auto length_line = ReadLine(input, cursor);
        if (!length_line.has_value())
        {
            result.status = ParseStatus::kIncomplete;
            return result;
        }

        std::size_t token_length = 0;
        if (!ParseNonNegativeInteger(length_line->line, token_length))
        {
            result.status = ParseStatus::kProtocolError;
            result.consumed_bytes = cursor + length_line->consumed;
            result.error_message = "invalid token length";
            return result;
        }

        cursor += length_line->consumed;

        if (cursor + token_length > input.size())
        {
            result.status = ParseStatus::kIncomplete;
            return result;
        }

        tokens.emplace_back(input.substr(cursor, token_length));
        cursor += token_length;

        if (!ConsumeLineEnding(input, cursor))
        {
            if (cursor >= input.size())
            {
                result.status = ParseStatus::kIncomplete;
            }
            else
            {
                result.status = ParseStatus::kProtocolError;
                result.consumed_bytes = cursor;
                result.error_message = "missing token terminator";
            }
            return result;
        }
    }

    result.status = ParseStatus::kOk;
    result.consumed_bytes = cursor;
    result.request.command = ToUpper(tokens.front());
    result.request.arguments.assign(tokens.begin() + 1, tokens.end());
    return result;
}
} // namespace KV
