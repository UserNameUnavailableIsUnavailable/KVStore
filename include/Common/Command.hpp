#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace KV
{
enum class CommandParseStatus
{
    kOk,
    kIncomplete,
    kProtocolError
};

struct CommandParseResult
{
    CommandParseStatus status = CommandParseStatus::kIncomplete;
    std::size_t consumed_bytes = 0;
    std::string error_message;
};

class Command
{
public:
    Command() = default;
    Command(std::string name, std::vector<std::string> arguments);

    const std::string& GetName() const;
    const std::vector<std::string>& GetArguments() const;
    std::string Serialize() const;
    CommandParseResult Deserialize(std::string_view input);

private:
    std::string name_;
    std::vector<std::string> arguments_;
};

} // KV

