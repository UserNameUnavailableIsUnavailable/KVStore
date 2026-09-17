#pragma once

#include <Application/RESP/RESP.hpp>

#include <Foundation/Core/Address.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace KV
{
// The command file a server reads at startup: one command per line, written the
// way it would be typed at a prompt. Configuring the server this way gives it
// one language instead of two -- whatever a client may send, a file may say, and
// the server applies it by running it through the same path.
//
// A line splits on whitespace. A double-quoted run is one argument, and inside it
// a backslash escapes the character after it, so a value with spaces in it can be
// written. Blank lines and lines whose first character is '#' are skipped, so a
// file can explain itself.
struct CommandLine
{
    // The file it came from, so a message about it can name the line instead of
    // leaving the reader to look for it.
    std::filesystem::path file;
    // 1-based, so the number is the one an editor shows.
    std::size_t number{0};
    // The line as written, trimmed: what a log line quotes back.
    std::string text;
    std::vector<std::string> arguments;

    // How a message names this line: "master.conf:2".
    std::string Where() const;
};

// Reads the file. Throws std::runtime_error when it cannot be opened: a command
// file is only ever named deliberately, so one that is not there is a mistake
// rather than a server that starts with half a configuration.
std::vector<CommandLine> ReadCommandFile(const std::filesystem::path &path);

// The request a line stands for: the RESP array a client would have sent, so the
// same validation and the same execution apply to both.
RESP::Object CommandRequest(const CommandLine &line);

// The lines of a file that are settings rather than commands. They are the facts
// a server has to know before it exists -- the port it answers clients on,
// whether and where it serves replicas, and the master it follows -- so they are
// read while it is being put together, and taken out of the list of commands:
//
//   config port <port>
//   config replication_address <ip> <port>
//   replicaof <ip> <port>       (or slaveof, the older name)
//
// Nothing means this file did not say: what the command line named wins over all
// of it, and a value no one named falls back to the server's own default.
struct StartupSettings
{
    std::optional<std::uint16_t> port;
    std::optional<std::uint16_t> replication_port;
    std::optional<std::string> replication_address;
    std::optional<Foundation::Core::Address> master;
};

// Reads those lines out of the file, and takes them out of `lines` so that what
// stays is only the commands. A keyword that names a setting but is not followed
// by what it wants throws std::runtime_error, and the message names the file and
// the line. A later line overrides an earlier one.
StartupSettings TakeStartupSettings(std::vector<CommandLine> &lines);
} // namespace KV
