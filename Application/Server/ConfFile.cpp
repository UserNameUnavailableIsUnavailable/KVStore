#include "ConfFile.hpp"

#include <Application/Commands.hpp>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace KV
{
namespace
{
constexpr std::string_view kWhitespace = " \t\r\v\f";

// Splits one line into arguments. Quotes and the escapes inside them are the
// only syntax the file has; everything else is an argument, backslashes outside
// quotes included -- a value is allowed to look like a path.
std::vector<std::string> SplitLine(std::string_view line)
{
    std::vector<std::string> arguments;
    std::string argument;
    bool quoted = false;
    // An argument can be empty and still be an argument: `set key ""` sets the
    // empty string, which is not the same as leaving the argument out.
    bool started = false;

    for (std::size_t index = 0; index < line.size(); ++index)
    {
        const char character = line[index];

        if (quoted)
        {
            if (character == '\\' && index + 1 < line.size())
            {
                argument.push_back(line[++index]);
                continue;
            }
            if (character == '"')
            {
                quoted = false;
                continue;
            }
            argument.push_back(character);
            continue;
        }

        // A quote opens a quoted run wherever it appears, so an argument can be
        // part quoted and part not. An unterminated one simply ends at the end of
        // the line: the command that follows will not validate, and saying which
        // line it was is the message.
        if (character == '"')
        {
            quoted = true;
            started = true;
            continue;
        }
        if (kWhitespace.find(character) != std::string_view::npos)
        {
            if (started)
            {
                arguments.push_back(std::move(argument));
                argument.clear();
                started = false;
            }
            continue;
        }

        argument.push_back(character);
        started = true;
    }

    if (started)
    {
        arguments.push_back(std::move(argument));
    }
    return arguments;
}

bool EqualWord(std::string_view left, std::string_view right)
{
    return std::ranges::equal(left, right, [](unsigned char a, unsigned char b) {
        return std::toupper(a) == std::toupper(b);
    });
}

// The port of a line that names one. Anything that is not a number in range is
// refused here rather than by a command that does not exist.
std::uint16_t ParsePort(const CommandLine &line, std::string_view text)
{
    const auto refuse = [&line]() -> std::runtime_error {
        return std::runtime_error(line.Where() + ": '" + line.arguments.front() + "' wants a port between 1 and 65535");
    };

    std::size_t consumed = 0;
    unsigned long port = 0;
    try
    {
        port = std::stoul(std::string{text}, &consumed);
    }
    catch (const std::exception &)
    {
        throw refuse();
    }
    if (consumed != text.size() || port == 0 || port > 65535)
    {
        throw refuse();
    }
    return static_cast<std::uint16_t>(port);
}

// The number a port that a command was already validated into is written as, so
// this cannot refuse a line the validator accepted.
std::uint16_t ToPort(std::string_view text)
{
    return static_cast<std::uint16_t>(std::stoul(std::string{text}));
}
} // namespace

std::string CommandLine::Where() const
{
    return file.string() + ":" + std::to_string(number);
}

std::vector<CommandLine> ReadCommandFile(const std::filesystem::path &path)
{
    std::ifstream file(path);
    if (!file) [[unlikely]]
    {
        throw std::runtime_error("cannot read the command file '" + path.string() + "'");
    }

    std::vector<CommandLine> lines;
    std::string text;
    std::size_t number = 0;
    while (std::getline(file, text))
    {
        ++number;

        const auto first = text.find_first_not_of(kWhitespace);
        if (first == std::string::npos || text[first] == '#')
        {
            continue; // a blank line, or one that explains the file
        }
        const auto last = text.find_last_not_of(kWhitespace);
        std::string trimmed = text.substr(first, last - first + 1);

        std::vector<std::string> arguments = SplitLine(trimmed);
        if (arguments.empty())
        {
            continue; // nothing but quotes, so there is no command on this line
        }
        lines.push_back(CommandLine{
            .file = path, .number = number, .text = std::move(trimmed), .arguments = std::move(arguments)});
    }
    return lines;
}

RESP::Object CommandRequest(const CommandLine &line)
{
    std::vector<RESP::Object> arguments;
    arguments.reserve(line.arguments.size());
    for (const std::string &argument : line.arguments)
    {
        arguments.push_back(RESP::Object(RESP::BulkString{.value = argument}));
    }
    return RESP::Object(RESP::Array{.values = std::move(arguments)});
}

StartupSettings TakeStartupSettings(std::vector<CommandLine> &lines)
{
    StartupSettings settings;

    for (auto line = lines.begin(); line != lines.end();)
    {
        if (line->arguments.empty())
        {
            ++line;
            continue;
        }

        // `replicaof <ip> <port>`, the command Redis spells that way, which is the
        // one command that cannot be run: it decides what this instance *is*.
        if (EqualWord(line->arguments.front(), "replicaof") || EqualWord(line->arguments.front(), "slaveof"))
        {
            if (line->arguments.size() != 3)
            {
                throw std::runtime_error(line->Where() + ": '" + line->arguments.front() + "' wants <ip> <port>");
            }
            try
            {
                settings.master = Foundation::Core::Address::from_ipv4(line->arguments[1], ParsePort(*line, line->arguments[2]));
            }
            catch (const std::exception &error)
            {
                throw std::runtime_error(line->Where() + ": " + error.what());
            }
            line = lines.erase(line);
            continue;
        }

        // `CONFIG <parameter> ...` for a parameter that has to be decided before
        // the server exists. The line is validated as the command it is, so a
        // setting and a command cannot disagree about what is well formed, and
        // anything else -- `config appendonly yes` included -- stays in the list
        // to be run once the server is up.
        const KV::CommandValidation validation = KV::ValidateCommand(CommandRequest(*line));
        if (!validation || validation.command->type != KV::CommandType::kConfig ||
            !KV::IsStartupConfigParameter(std::get<KV::ConfigParams>(validation.command->parameters).parameter))
        {
            ++line; // not a setting: this one is a command, and it stays where it is
            continue;
        }

        const auto &config = std::get<KV::ConfigParams>(validation.command->parameters);
        if (config.parameter == "port")
        {
            settings.port = ToPort(config.values.front());
        }
        else
        {
            settings.replication_address = config.values.front();
            settings.replication_port = ToPort(config.values.back());
        }

        // A later line overrides an earlier one, the way the rest of a file reads,
        // and every one of them leaves the list: they are settings, not commands.
        line = lines.erase(line);
    }

    return settings;
}
} // namespace KV
