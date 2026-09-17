#pragma once

#include <Application/RESP/RESP.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace KV
{
enum class CommandType
{
    kPing,
    kGet,
    kSet,
    kDel,
    kExists,
    kDbSize,
    kExpire,
    kTTL,
    kMulti,
    kExec,
    kCommand,
    kClient,
    kConfig,
    kBgSave,
};

// A command that mutates the store. The AOF records these, the replication log
// tracks them, and a read-only replica refuses them, so all three ask the same
// question through this one answer.
bool IsWriteCommand(CommandType type) noexcept;

// True for the CONFIG parameters that can only be decided before a server
// exists: the port clients reach it on, and the address and port it serves
// replicas from. A command file may name them while the server is being put
// together, and a running server refuses them, because there is nothing left to
// rebind. The name is the one a CONFIG command was validated into, in lower
// case.
bool IsStartupConfigParameter(std::string_view name) noexcept;

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

// `DBSIZE` answers with the number of keys the store holds, so it carries no
// arguments of its own.
struct DbSizeParams
{
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

// `COMMAND` and its subcommands only describe the command table. This server
// has no metadata to publish, but it still has to answer: redis-cli probes
// `COMMAND DOCS` the moment it connects.
struct CommandParams
{
};

// `CONFIG GET <parameter>` / `CONFIG SET <parameter> <value>...`, spelled the
// way Redis spells them. No values is what tells the read form from the write
// form, and a parameter that takes more than one value gets all of them:
// `replication_address` wants an address and a port.
struct ConfigParams
{
    std::string parameter;
    std::vector<std::string> values;
};

// `CLIENT` describes the connection. Nothing here is stateful: it exists
// because clients announce themselves on connect (`CLIENT SETINFO`), and a
// client that gets an error instead treats the connection as unusable.
struct ClientParams
{
    std::string subcommand;
    std::vector<std::string> arguments;
};

// The snapshot command forks a child, so it is named the way Redis names a
// fork-then-write: `BGSAVE`.
struct BgSaveParams
{
};

using Parameters = std::variant<PingParams, GetParams, SetParams, DelParams, ExistsParams, DbSizeParams, ExpireParams, TTLParams,
                                MultiParams, ExecParams, CommandParams, ClientParams, ConfigParams, BgSaveParams>;

struct Command
{
    CommandType type;
    Parameters parameters;
};

// The command's name as it appears on the wire, e.g. "BGSAVE".
std::string_view CommandName(CommandType type);

// The command as the RESP array that would reproduce it.
RESP::Object CommandToRESP(const Command &command);

// The RESP wire bytes of `CommandToRESP(command)`. The AOF needs an object so
// its encoder can stream it; replication needs bytes it can hand to the wire.
std::string EncodeCommand(const Command &command);

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