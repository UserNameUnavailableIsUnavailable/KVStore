#include <gtest/gtest.h>

#include <Application/commands.hpp>
#include <array>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

static_assert(std::is_same_v<KV::CommandValidation::value_type, void>);
static_assert(std::is_same_v<KV::CommandValidation::error_type, std::string>);

namespace {
RESP::Object Request(std::span<const std::string_view> arguments) {
    RESP::Array array;
    for (const std::string_view argument : arguments) {
        array.values.emplace_back(RESP::BulkString{.value = std::string(argument)});
    }
    return RESP::Object(std::move(array));
}
}  // namespace

TEST(CommandTesting, ValidationAndParsingAcceptTheSameCommands) {
    struct Case {
        std::vector<std::string_view> arguments;
        KV::CommandType type;
    };
    const std::vector<Case> cases = {
        {{"ping"}, KV::CommandType::kPing},
        {{"GET", "key"}, KV::CommandType::kGet},
        {{"sEt", "key", "value"}, KV::CommandType::kSet},
        {{"SET", "", ""}, KV::CommandType::kSet},
        {{"DEL", "key"}, KV::CommandType::kDel},
        {{"EXISTS", "key"}, KV::CommandType::kExists},
        {{"DBSIZE"}, KV::CommandType::kDbSize},
        {{"MULTI"}, KV::CommandType::kMulti},
        {{"EXEC"}, KV::CommandType::kExec},
        {{"COMMAND", "DOCS"}, KV::CommandType::kCommand},
        {{"INFO"}, KV::CommandType::kInfo},
        {{"BGSAVE"}, KV::CommandType::kBgSave},
        {{"SAVE"}, KV::CommandType::kSave},
        {{"CONFIG", "GET", "unknown"}, KV::CommandType::kConfig},
        {{"CONFIG", "SET", "APPENDONLY", "YES"}, KV::CommandType::kConfig},
        {{"CONFIG", "aof_checksum", "NO"}, KV::CommandType::kConfig},
        {{"CONFIG", "port", "6666"}, KV::CommandType::kConfig},
        {{"CONFIG", "rdma_device", "SIW0"}, KV::CommandType::kConfig},
        {{"CONFIG", "replication_address", "192.168.4.102", "0"}, KV::CommandType::kConfig},
        {{"CONFIG", "save", "0", "1000"}, KV::CommandType::kConfig},
        {{"CLIENT", "SETINFO", "LIB-NAME", "Test"}, KV::CommandType::kClient},
        {{"CLIENT", "setname", "Test"}, KV::CommandType::kClient},
        {{"CLIENT", "getname"}, KV::CommandType::kClient},
        {{"CLIENT", "id"}, KV::CommandType::kClient},
        {{"CLIENT", "info"}, KV::CommandType::kClient},
        {{"CLIENT", "list"}, KV::CommandType::kClient},
        {{"CLIENT", "no-evict", "on"}, KV::CommandType::kClient},
        {{"CLIENT", "no-touch", "on"}, KV::CommandType::kClient},
        {{"SLAVEOF", "192.168.4.102", "6667"}, KV::CommandType::kSlaveOf},
    };
    for (const Case& test : cases) {
        SCOPED_TRACE(test.arguments.front());
        const std::span<const std::string_view> arguments = test.arguments;
        const RESP::Object request = Request(test.arguments);
        ASSERT_TRUE(KV::ValidateCommand(arguments));
        ASSERT_TRUE(KV::ValidateCommand(request));
        const auto from_views = KV::ParseCommand(arguments);
        const auto from_object = KV::ParseCommand(request);
        ASSERT_TRUE(from_views);
        ASSERT_TRUE(from_object);
        EXPECT_EQ(from_views->type, test.type);
        EXPECT_EQ(from_object->type, test.type);
        EXPECT_EQ(KV::EncodeCommand(*from_views), KV::EncodeCommand(*from_object));
    }
}

TEST(CommandTesting, ValidationAndParsingPreserveErrors) {
    struct Case {
        std::vector<std::string_view> arguments;
        std::string_view error;
    };
    const std::vector<Case> cases = {
        {{}, "ERR command must be a non-empty RESP array"},
        {{"unknown"}, "ERR unknown command 'unknown'"},
        {{"PING", "extra"}, "ERR wrong number of arguments for 'PING'"},
        {{"GET"}, "ERR wrong number of arguments for 'GET'"},
        {{"SET", "key"}, "ERR wrong number of arguments for 'SET'"},
        {{"SET", "key", "value", "extra"}, "ERR wrong number of arguments for 'SET'"},
        {{"DEL"}, "ERR wrong number of arguments for 'DEL'"},
        {{"EXISTS"}, "ERR wrong number of arguments for 'EXISTS'"},
        {{"DBSIZE", "extra"}, "ERR wrong number of arguments for 'DBSIZE'"},
        {{"MULTI", "extra"}, "ERR wrong number of arguments for 'MULTI'"},
        {{"EXEC", "extra"}, "ERR wrong number of arguments for 'EXEC'"},
        {{"INFO", "extra"}, "ERR wrong number of arguments for 'INFO'"},
        {{"BGSAVE", "extra"}, "ERR wrong number of arguments for 'BGSAVE'"},
        {{"SAVE", "extra"}, "ERR wrong number of arguments for 'SAVE'"},
        {{"CONFIG"}, "ERR wrong number of arguments for 'CONFIG'"},
        {{"CONFIG", "GET"}, "ERR Unknown subcommand or wrong number of arguments for 'GET'. Try CONFIG HELP."},
        {{"CONFIG", "SET", "port"}, "ERR Unknown subcommand or wrong number of arguments for 'SET'. Try CONFIG HELP."},
        {{"CONFIG", "SET", "UNKNOWN", "value"}, "ERR Unknown option or number of arguments for CONFIG SET - 'unknown'"},
        {{"CONFIG", "SET", "appendonly", "invalid"},
         "ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'"},
        {{"CONFIG", "port", "70000"}, "ERR CONFIG SET failed - 'port' wants a port between 1 and 65535"},
        {{"CONFIG", "rdma_device", ""},
         "ERR CONFIG SET failed - 'rdma_device' wants the name of the rdma device replication runs on"},
        {{"CONFIG", "replication_address", "0.0.0.0", "6667"},
         "ERR CONFIG SET failed - 'replication_address' wants <ip> <port>, the address of the rdma device to serve "
         "replicas from"},
        {{"CONFIG", "save", "0", "0"}, "ERR CONFIG SET failed - 'save' wants <seconds> <changed>, with changed > 0"},
        {{"CONFIG", "appendfsync", "always"}, "ERR CONFIG SET failed - 'appendfsync' is read-only on this server"},
        {{"CLIENT"}, "ERR wrong number of arguments for 'CLIENT'"},
        {{"CLIENT", "UNKNOWN"}, "ERR Unknown subcommand or wrong number of arguments for 'unknown'. Try CLIENT HELP."},
        {{"CLIENT", "setinfo"}, "ERR Unknown subcommand or wrong number of arguments for 'setinfo'. Try CLIENT HELP."},
        {{"SLAVEOF"}, "ERR wrong number of arguments for 'SLAVEOF'"},
        {{"SLAVEOF", "not-an-address", "6667"},
         "ERR SLAVEOF wants <ip> <port>, the rdma address of the master to follow"},
        {{"SLAVEOF", "192.168.4.102", "0"}, "ERR SLAVEOF wants <ip> <port>, the rdma address of the master to follow"},
    };
    for (const Case& test : cases) {
        SCOPED_TRACE(test.error);
        const std::span<const std::string_view> arguments = test.arguments;
        const RESP::Object request = Request(test.arguments);
        const auto validation = KV::ValidateCommand(arguments);
        const auto object_validation = KV::ValidateCommand(request);
        const auto parsed = KV::ParseCommand(arguments);
        const auto object_parsed = KV::ParseCommand(request);
        ASSERT_FALSE(validation);
        ASSERT_FALSE(object_validation);
        ASSERT_FALSE(parsed);
        ASSERT_FALSE(object_parsed);
        EXPECT_EQ(validation.error(), test.error);
        EXPECT_EQ(object_validation.error(), test.error);
        EXPECT_EQ(parsed.error(), test.error);
        EXPECT_EQ(object_parsed.error(), test.error);
    }
}

TEST(CommandTesting, MalformedObjectsAreRejected) {
    const std::vector<RESP::Object> requests = {
        RESP::Object(RESP::SimpleString{.value = "SET"}),
        RESP::Object(RESP::Array{.values = {RESP::Object(RESP::Integer{.value = 1})}}),
        RESP::Object(RESP::Array{.values = {RESP::Object(RESP::BulkString{.value = std::nullopt})}}),
    };
    for (std::size_t index = 0; index < requests.size(); ++index) {
        const auto validation = KV::ValidateCommand(requests[index]);
        const auto parsed = KV::ParseCommand(requests[index]);
        ASSERT_FALSE(validation);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(validation.error(),
                  index == 0 ? "ERR command must be a non-empty RESP array" : "ERR command arguments must be strings");
        EXPECT_EQ(parsed.error(), validation.error());
    }
}

TEST(CommandTesting, ParsingOwnsBinaryArguments) {
    std::string key(256, 'k');
    std::string value("a\0b", 3);
    const std::array<std::string_view, 3> arguments{"SET", key, value};
    const std::span<const std::string_view> views = arguments;
    ASSERT_TRUE(KV::ValidateCommand(views));
    const auto command = KV::ParseCommand(views);
    ASSERT_TRUE(command);
    key.assign(256, 'x');
    value.assign("changed");
    const auto& set = std::get<KV::SetParams>(command->parameters);
    EXPECT_EQ(set.key, std::string(256, 'k'));
    EXPECT_EQ(set.value, std::string("a\0b", 3));
}

TEST(CommandTesting, ParsingPreservesNormalizationAndClientArguments) {
    const std::array<std::string_view, 4> config_arguments{"CONFIG", "SET", "RDMA_DEVICE", "SIW0"};
    const auto config = KV::ParseCommand(std::span<const std::string_view>(config_arguments));
    ASSERT_TRUE(config);
    const auto& parameters = std::get<KV::ConfigParams>(config->parameters);
    EXPECT_EQ(parameters.parameter, "rdma_device");
    EXPECT_EQ(parameters.values, (std::vector<std::string>{"siw0"}));

    const std::array<std::string_view, 4> client_arguments{"CLIENT", "SETINFO", "LIB-NAME", "Test"};
    const auto client = KV::ParseCommand(std::span<const std::string_view>(client_arguments));
    ASSERT_TRUE(client);
    const auto& client_parameters = std::get<KV::ClientParams>(client->parameters);
    EXPECT_EQ(client_parameters.subcommand, "setinfo");
    EXPECT_EQ(client_parameters.arguments, (std::vector<std::string>{"LIB-NAME", "Test"}));
}
