#pragma once

#include <Application/RESP/RESP.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <variant>

namespace KV
{
enum class CommandType
{
    kPing,
    kGet,
    kSet,
    kDel,
    kExists,
    kExpire,
    kTTL,
    kMulti,
    kExec,
    kAppendOnly,
    kSave,
};

struct PingParams
{
};

struct GetParams
{
    std::string key;
};

struct SetParams
{
    std::string key;
    std::string value;
};

struct DelParams
{
    std::string key;
};

struct ExistsParams
{
    std::string key;
};

struct ExpireParams
{
    std::string key;
    std::chrono::milliseconds ttl;
};

struct TTLParams
{
};

struct MultiParams
{
};

struct ExecParams
{
};

struct AppendOnlyParams
{
    bool enabled;
};

struct SaveParams
{
};

using Parameters = std::variant<PingParams, GetParams, SetParams, DelParams, ExistsParams, ExpireParams, TTLParams, MultiParams, ExecParams,
                                AppendOnlyParams, SaveParams>;

struct Command
{
    CommandType type;
    Parameters parameters;
};

struct CommandValidation
{
    std::optional<Command> command;
    std::string error;

    explicit operator bool() const noexcept
    {
        return command.has_value();
    }
};

CommandValidation ValidateCommand(const RESP::Object &request);
} // namespace KV