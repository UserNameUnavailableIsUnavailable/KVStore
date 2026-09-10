#include <filesystem>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "Protocol.hpp"
#include "Server/Server.hpp"

namespace
{
class TestServer final : public KV::Server
{
public:
    using Server::Server;
    using Server::Execute;

    void run(std::uint16_t, int) override {}
};

KV::Command MakeCommand(std::string_view name, std::initializer_list<std::string_view> arguments = {})
{
    KV::Command command {.name = std::pmr::string(name), .arguments = std::pmr::vector<std::pmr::string>()};
    for (const std::string_view argument : arguments)
    {
        command.arguments.emplace_back(argument);
    }
    return command;
}

std::filesystem::path PersistencePath(std::string_view test_name)
{
    return std::filesystem::temp_directory_path() / ("kvstore-" + std::string(test_name));
}

std::vector<KV::Command> DecodeCommandStream(std::string_view stream)
{
    KV::Protocol protocol;
    std::vector<KV::Command> commands;
    std::size_t offset = 0;
    while (offset < stream.size())
    {
        KV::RequestDecode decoded = protocol.DecodeRequest(stream.substr(offset));
        if (decoded.status != KV::DecodeStatus::kComplete)
        {
            return {};
        }
        offset += decoded.consumed_bytes;
        commands.push_back(std::move(decoded.command));
    }
    return commands;
}
} // namespace

TEST(ServerPersistenceTesting, IncrementalLogRestoresMutations)
{
    const std::filesystem::path path = PersistencePath("incremental");
    std::filesystem::remove_all(path);
    {
        TestServer server(KV::CacheStrategy::kHash, path.string());
        ASSERT_EQ(server.Execute(MakeCommand("APPendONLY", {"YES"})).type, KV::ResultType::kSimpleString);
        EXPECT_EQ(server.Execute(MakeCommand("SET", {"saved", "value"})).type, KV::ResultType::kSimpleString);
        EXPECT_EQ(server.Execute(MakeCommand("SET", {"deleted", "value"})).type, KV::ResultType::kSimpleString);
        EXPECT_EQ(server.Execute(MakeCommand("DELETE", {"deleted"})).type, KV::ResultType::kSimpleString);
    }

    const auto aof = std::filesystem::directory_iterator(path / "aof");
    ASSERT_NE(aof, std::filesystem::directory_iterator());
    EXPECT_EQ(aof->path().extension(), ".aof");

    TestServer restored(KV::CacheStrategy::kHash, path.string());
    const KV::Result saved = restored.Execute(MakeCommand("GET", {"saved"}));
    EXPECT_EQ(saved.type, KV::ResultType::kBulkString);
    EXPECT_EQ(saved.value, "value");
    EXPECT_EQ(restored.Execute(MakeCommand("GET", {"deleted"})).type, KV::ResultType::kError);
    std::filesystem::remove_all(path);
}

TEST(ServerPersistenceTesting, FullSaveRestoresRemainingTtl)
{
    const std::filesystem::path path = PersistencePath("full");
    std::filesystem::remove_all(path);
    {
        TestServer server(KV::CacheStrategy::kHash, path.string());
        ASSERT_EQ(server.Execute(MakeCommand("SET", {"timed", "value"})).type, KV::ResultType::kSimpleString);
        ASSERT_EQ(server.Execute(MakeCommand("EXPIRE", {"timed", "5000"})).type, KV::ResultType::kSimpleString);
        ASSERT_EQ(server.Execute(MakeCommand("SET", {"permanent", "value"})).type, KV::ResultType::kSimpleString);
        EXPECT_EQ(server.Execute(MakeCommand("SAVE")).type, KV::ResultType::kSimpleString);
    }

    const auto full = std::filesystem::directory_iterator(path / "full");
    ASSERT_NE(full, std::filesystem::directory_iterator());
    EXPECT_EQ(full->path().extension(), ".ful");

    TestServer restored(KV::CacheStrategy::kHash, path.string());
    EXPECT_EQ(restored.Execute(MakeCommand("GET", {"timed"})).value, "value");
    EXPECT_EQ(restored.Execute(MakeCommand("GET", {"permanent"})).value, "value");
    const KV::Result ttl = restored.Execute(MakeCommand("TTL", {"timed"}));
    EXPECT_EQ(ttl.type, KV::ResultType::kSimpleString);
    EXPECT_GT(std::stoll(std::string(ttl.value)), 0);
    std::filesystem::remove_all(path);
}

TEST(ServerReplicationTesting, PsyncProvidesFullThenPartialCommandStreams)
{
    const std::filesystem::path path = PersistencePath("psync");
    std::filesystem::remove_all(path);
    TestServer server(KV::CacheStrategy::kHash, path.string());
    ASSERT_EQ(server.Execute(MakeCommand("SET", {"first", "value"})).type, KV::ResultType::kSimpleString);

    const KV::Result full = server.Execute(MakeCommand("PSYNC", {"?", "0"}));
    ASSERT_EQ(full.type, KV::ResultType::kArray);
    ASSERT_EQ(full.elements.size(), 4U);
    EXPECT_EQ(full.elements[0].value, "FULLRESYNC");
    EXPECT_EQ(DecodeCommandStream(full.elements[3].value).size(), 3U);

    ASSERT_EQ(server.Execute(MakeCommand("SET", {"second", "value"})).type, KV::ResultType::kSimpleString);
    const KV::Result partial = server.Execute(MakeCommand("PSYNC", {
        std::string_view(full.elements[1].value), std::string_view(full.elements[2].value)}));
    ASSERT_EQ(partial.type, KV::ResultType::kArray);
    ASSERT_EQ(partial.elements.size(), 4U);
    EXPECT_EQ(partial.elements[0].value, "CONTINUE");
    const std::vector<KV::Command> commands = DecodeCommandStream(partial.elements[3].value);
    ASSERT_EQ(commands.size(), 1U);
    EXPECT_EQ(commands[0].name, "SET");
    EXPECT_EQ(commands[0].arguments[0], "second");
    std::filesystem::remove_all(path);
}

TEST(ServerReplicationTesting, SlaveofRejectsInvalidPort)
{
    TestServer server;
    const KV::Result result = server.Execute(MakeCommand("SLAVEOF", {"127.0.0.1", "invalid"}));
    EXPECT_EQ(result.type, KV::ResultType::kError);
}

TEST(ServerReplicationTesting, SlaveofRejectsItsOwnListener)
{
    TestServer server;
    server.Listen(18081);

    const KV::Result result = server.Execute(MakeCommand("SLAVEOF", {"127.0.0.1", "18081"}));

    EXPECT_EQ(result.type, KV::ResultType::kError);
    EXPECT_EQ(result.value, "server cannot replicate from itself");
}
