#include <gtest/gtest.h>

#include <Application/Commands.hpp>
#include <Application/Server/ConfFile.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace
{
using KV::CommandLine;

// Reads a file of this exact text, so a test never depends on one it did not
// write and never leaves one behind.
std::vector<CommandLine> ReadLines(std::string_view contents)
{
    const auto path =
        std::filesystem::temp_directory_path() / ("kvstore-conf-" + std::to_string(std::random_device{}()) + ".conf");
    {
        std::ofstream file(path, std::ios::binary);
        file << contents;
    }
    std::vector<CommandLine> lines = KV::ReadCommandFile(path);
    std::filesystem::remove(path);
    return lines;
}

// The arguments of the only line of a one-line file.
std::vector<std::string> Arguments(std::string_view one_line)
{
    const std::vector<CommandLine> lines = ReadLines(one_line);
    return lines.empty() ? std::vector<std::string>{} : lines.front().arguments;
}

// The command a line stands for, as the server would see it.
std::optional<KV::Command> CommandOf(const std::vector<CommandLine> &lines, std::size_t index = 0)
{
    return KV::ValidateCommand(KV::CommandRequest(lines[index])).command;
}
} // namespace

TEST(ConfFileTesting, TheMastersFileIsOneCommand)
{
    // The shape a real file has: the command as it would be typed, in lower case.
    const std::vector<CommandLine> lines = ReadLines("config appendonly yes\n");
    ASSERT_EQ(lines.size(), 1u);
    EXPECT_EQ(lines.front().number, 1u);
    EXPECT_EQ(lines.front().text, "config appendonly yes");

    const auto command = CommandOf(lines);
    ASSERT_TRUE(command.has_value());
    ASSERT_EQ(command->type, KV::CommandType::kConfig);
    const auto &config = std::get<KV::ConfigParams>(command->parameters);
    EXPECT_EQ(config.parameter, "appendonly");
    ASSERT_EQ(config.values.size(), 1u);
    EXPECT_EQ(config.values.front(), "yes");
}

TEST(ConfFileTesting, CommentsAndBlankLinesAreNotCommands)
{
    const std::vector<CommandLine> lines = ReadLines("  # the master\n"
                                                     "\n"
                                                     "\t\n"
                                                     "config appendonly yes\n"
                                                     "   # trailing note\n"
                                                     "dbsize\n");
    ASSERT_EQ(lines.size(), 2u);
    EXPECT_EQ(lines[0].text, "config appendonly yes");
    EXPECT_EQ(lines[0].number, 4u) << "the number is the line in the file, not in the commands";
    EXPECT_EQ(lines[1].text, "dbsize");
    EXPECT_EQ(lines[1].number, 6u);
}

TEST(ConfFileTesting, ALineSplitsOnWhitespace)
{
    EXPECT_EQ(Arguments("config   appendonly\tyes\n"), (std::vector<std::string>{"config", "appendonly", "yes"}));
    EXPECT_EQ(Arguments("\t set  key  value \n"), (std::vector<std::string>{"set", "key", "value"}));
}

TEST(ConfFileTesting, AQuotedRunIsOneArgument)
{
    EXPECT_EQ(Arguments(R"(set greeting "hello world")" "\n"), (std::vector<std::string>{"set", "greeting", "hello world"}));

    // An empty argument is still an argument: `set key ""` writes the empty
    // string, which is not the same as leaving the value out.
    EXPECT_EQ(Arguments(R"(set key "")" "\n"), (std::vector<std::string>{"set", "key", ""}));

    // A quote can open in the middle of an argument.
    EXPECT_EQ(Arguments(R"(set key a" b"c)" "\n"), (std::vector<std::string>{"set", "key", "a bc"}));
}

TEST(ConfFileTesting, ABackslashEscapesOnlyInsideQuotes)
{
    // The line `set key "a\"b"` asks for the argument `a"b`.
    EXPECT_EQ(Arguments(R"(set key "a\"b")" "\n"), (std::vector<std::string>{"set", "key", "a\"b"}));
    EXPECT_EQ(Arguments(R"(set key "a\\b")" "\n"), (std::vector<std::string>{"set", "key", "a\\b"}));

    // Outside quotes a backslash is an ordinary character, so a value is allowed
    // to look like a path.
    EXPECT_EQ(Arguments(R"(set key C:\logs\today)" "\n"), (std::vector<std::string>{"set", "key", "C:\\logs\\today"}));
}

TEST(ConfFileTesting, TheEndOfALineDoesNotBecomeAnArgument)
{
    // A file written on Windows has to read the same as one written here.
    EXPECT_EQ(Arguments("config appendonly yes\r\n"), (std::vector<std::string>{"config", "appendonly", "yes"}));
}

TEST(ConfFileTesting, TheLinesAreCommandsTheServerKnows)
{
    const std::vector<CommandLine> lines = ReadLines("info\n"
                                                     "set greeting \"hello world\"\n"
                                                     "del greeting\n"
                                                     "config appendonly no\n");
    ASSERT_EQ(lines.size(), 4u);

    const auto info = CommandOf(lines, 0);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->type, KV::CommandType::kInfo);

    const auto set = CommandOf(lines, 1);
    ASSERT_TRUE(set.has_value());
    ASSERT_EQ(set->type, KV::CommandType::kSet);
    EXPECT_EQ(std::get<KV::SetParams>(set->parameters).value, "hello world") << "quoting survives into the command";

    const auto del = CommandOf(lines, 2);
    ASSERT_TRUE(del.has_value());
    EXPECT_EQ(del->type, KV::CommandType::kDel);

    const auto config = CommandOf(lines, 3);
    ASSERT_TRUE(config.has_value());
    ASSERT_EQ(config->type, KV::CommandType::kConfig);
    const auto &parameters = std::get<KV::ConfigParams>(config->parameters);
    ASSERT_EQ(parameters.values.size(), 1u);
    EXPECT_EQ(parameters.values.front(), "no");
}

TEST(ConfFileTesting, ALineTheServerDoesNotKnowIsRefused)
{
    // Nothing here decides what to do about it: the message is what the startup
    // reports, and it names the command rather than the line, which is what a
    // client would have been told.
    const std::vector<CommandLine> lines = ReadLines("frobnicate now\n");
    ASSERT_EQ(lines.size(), 1u);

    const KV::CommandValidation validation = KV::ValidateCommand(KV::CommandRequest(lines.front()));
    EXPECT_FALSE(validation);
    EXPECT_NE(validation.error.find("unknown command 'frobnicate'"), std::string::npos);
}

TEST(ConfFileTesting, TheSizeCommandTakesNothing)
{
    const std::vector<CommandLine> lines = ReadLines("DBSIZE\n");
    ASSERT_EQ(lines.size(), 1u);

    const auto command = CommandOf(lines);
    ASSERT_TRUE(command.has_value());
    EXPECT_EQ(command->type, KV::CommandType::kDbSize);
    EXPECT_EQ(KV::CommandName(command->type), "DBSIZE");

    const std::vector<CommandLine> wrong = ReadLines("dbsize extra\n");
    const KV::CommandValidation validation = KV::ValidateCommand(KV::CommandRequest(wrong.front()));
    EXPECT_FALSE(validation);
    EXPECT_NE(validation.error.find("wrong number of arguments"), std::string::npos);
}

TEST(ConfFileTesting, AFileThatIsNotThereIsRefused)
{
    const auto path = std::filesystem::temp_directory_path() / "kvstore-conf-that-is-not-there.conf";
    std::filesystem::remove(path);
    EXPECT_THROW(KV::ReadCommandFile(path), std::runtime_error);
}

TEST(ConfFileTesting, AFileCanNameTheMasterToFollow)
{
    std::vector<CommandLine> lines = ReadLines("replicaof 127.0.0.1 8081\n");
    ASSERT_EQ(lines.size(), 1u);

    const KV::StartupSettings settings = KV::TakeStartupSettings(lines);
    ASSERT_TRUE(settings.master.has_value());
    EXPECT_EQ(settings.master->ip(), "127.0.0.1");
    EXPECT_EQ(settings.master->port(), 8081);
    EXPECT_TRUE(lines.empty()) << "a setting is taken out of the file, not run as a command";
}

TEST(ConfFileTesting, ThePortsCanComeFromTheFile)
{
    // The spelling the file wants: the same `CONFIG` command a client could send,
    // with the port and the address of the RDMA device to serve replicas from.
    std::vector<CommandLine> lines = ReadLines("config port 8082\n"
                                               "config replication_address 192.168.0.201 8081\n");

    const KV::StartupSettings settings = KV::TakeStartupSettings(lines);
    ASSERT_TRUE(settings.port.has_value());
    EXPECT_EQ(*settings.port, 8082);
    ASSERT_TRUE(settings.replication_port.has_value());
    EXPECT_EQ(*settings.replication_port, 8081);
    ASSERT_TRUE(settings.replication_address.has_value());
    EXPECT_EQ(*settings.replication_address, "192.168.0.201");
    EXPECT_FALSE(settings.master.has_value()) << "a file says only what it says";
    EXPECT_TRUE(lines.empty());
}

TEST(ConfFileTesting, TheRestOfTheFileIsStillCommands)
{
    std::vector<CommandLine> lines = ReadLines("  # this node follows another\n"
                                               "replicaof 127.0.0.1 8081\n"
                                               "config port 8082\n"
                                               "config appendonly yes\n");
    ASSERT_EQ(lines.size(), 3u);

    const KV::StartupSettings settings = KV::TakeStartupSettings(lines);
    ASSERT_TRUE(settings.master.has_value());
    EXPECT_EQ(settings.master->port(), 8081);
    ASSERT_TRUE(settings.port.has_value());
    EXPECT_EQ(*settings.port, 8082);
    ASSERT_EQ(lines.size(), 1u) << "only the settings left the list";
    EXPECT_EQ(lines.front().text, "config appendonly yes");

    const auto command = CommandOf(lines);
    ASSERT_TRUE(command.has_value());
    EXPECT_EQ(command->type, KV::CommandType::kConfig);
}

TEST(ConfFileTesting, TheLaterSettingWins)
{
    // A file that names a setting twice means the last one, the way the rest of a
    // file reads.
    std::vector<CommandLine> lines = ReadLines("config port 8082\n"
                                               "config port 8083\n");
    const KV::StartupSettings settings = KV::TakeStartupSettings(lines);
    ASSERT_TRUE(settings.port.has_value());
    EXPECT_EQ(*settings.port, 8083);
    EXPECT_TRUE(lines.empty());
}

TEST(ConfFileTesting, AMasterThatIsNotAnSocketAddressIsRefused)
{
    std::vector<CommandLine> no_port = ReadLines("replicaof 127.0.0.1\n");
    EXPECT_THROW(KV::TakeStartupSettings(no_port), std::runtime_error);
    EXPECT_EQ(no_port.size(), 1u) << "nothing was taken from the file";

    std::vector<CommandLine> wide_port = ReadLines("  replicaof 127.0.0.1 70000\n");
    EXPECT_THROW(KV::TakeStartupSettings(wide_port), std::runtime_error);

    std::vector<CommandLine> bad_ip = ReadLines("replicaof not-an-address 8081\n");
    EXPECT_THROW(KV::TakeStartupSettings(bad_ip), std::runtime_error);
}

TEST(ConfFileTesting, ASettingWithoutItsValueIsRefused)
{
    // A setting is still a command, so one that does not carry what it wants is
    // refused by the command validator -- and, refusing it, is left in the list
    // where the startup reports it with the line named.
    std::vector<CommandLine> alone = ReadLines("config port\n");
    KV::TakeStartupSettings(alone);
    ASSERT_EQ(alone.size(), 1u) << "nothing was taken from the file";
    EXPECT_FALSE(KV::ValidateCommand(KV::CommandRequest(alone.front())));

    std::vector<CommandLine> no_port = ReadLines("config replication_address 192.168.0.201\n");
    KV::TakeStartupSettings(no_port);
    ASSERT_EQ(no_port.size(), 1u);
    EXPECT_FALSE(KV::ValidateCommand(KV::CommandRequest(no_port.front())));
}

TEST(ConfFileTesting, ASettingTheServerCannotHonourIsRefused)
{
    const auto refused = [](std::string_view line) {
        std::vector<CommandLine> lines = ReadLines(line);
        KV::TakeStartupSettings(lines);
        return !KV::ValidateCommand(KV::CommandRequest(lines.front())) && lines.size() == 1u;
    };

    EXPECT_TRUE(refused("config port 70000\n")) << "a port out of range";
    EXPECT_TRUE(refused("config port 0\n"));
    EXPECT_TRUE(refused("config replication_address 192.168.0.201 70000\n"));
    EXPECT_TRUE(refused("config replication_address not-an-address 8081\n"));
    EXPECT_TRUE(refused("config replication_address 0.0.0.0 8081\n")) << "a wildcard names no RDMA device";
    EXPECT_TRUE(refused("config replication_address 192.168.0.201:8081 1\n"));
}
