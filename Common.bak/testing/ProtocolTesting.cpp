#include "Address.hpp"
#include "Protocol.hpp"
#include "Session.hpp"

#include <gtest/gtest.h>
#include <netiin.h>

#include <cstring>
#include <memory_resource>
#include <string>

namespace
{
KV::Command MakeCommand(std::pmr::memory_resource* resource, std::string_view name,
    std::initializer_list<std::string_view> arguments = {})
{
    KV::Command command {.name = std::pmr::string(name, resource),
        .arguments = std::pmr::vector<std::pmr::string>(resource)};
    for (const std::string_view argument : arguments)
    {
        command.arguments.emplace_back(argument);
    }
    return command;
}

KV::Result MakeResult(std::pmr::memory_resource* resource, KV::ResultType type, std::string_view value)
{
    return {.type = type, .value = std::pmr::string(value, resource), .elements = std::pmr::vector<KV::Result>(resource)};
}

class TestSession final : public KV::Session
{
public:
    TestSession() : Session(KV::NetworkingModel::kReactor) {}

    void Receive(std::string_view bytes)
    {
        const std::span<char> destination = PrepareReceive();
        std::memcpy(destination.data(), bytes.data(), bytes.size());
        CompleteReceive(static_cast<int>(bytes.size()));
    }
};
} // namespace

TEST(ProtocolTesting, EncodesRequestAsRespArray)
{
    KV::Protocol protocol;
    const KV::Command command = MakeCommand(std::pmr::get_default_resource(), "SET", {"key", "value"});

    EXPECT_EQ(protocol.EncodeRequest(command), "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
}

TEST(AddressTesting, ExposesNativeAddressAndReadableIpv4Details)
{
    KVFoundation::Address address;

    // Write through the raw accessor: the caller is responsible for setting
    // the family, the matching size, the port, and the IP.
    ::sockaddr_in& ipv4 = address.Storage<KVFoundation::Address::IPv4StorageType>();
    ipv4.sin_family = AF_INET;
    ipv4.sin_port = htons(6379);
    ASSERT_EQ(::inet_pton(AF_INET, "127.0.0.1", &ipv4.sin_addr), 1);
    address.GetSize() = sizeof(::sockaddr_in);

    EXPECT_EQ(address.GetSize(), sizeof(::sockaddr_in));
    EXPECT_EQ(address.GetIP(), "127.0.0.1");
    EXPECT_EQ(address.GetPort(), 6379);
}

TEST(ProtocolTesting, DecodesIncompleteRequestThenCompleteRequest)
{
    KV::Protocol protocol;
    const std::string wire = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

    EXPECT_EQ(protocol.DecodeRequest(wire.substr(0, 12)).status, KV::DecodeStatus::kIncomplete);
    const KV::RequestDecode decoded = protocol.DecodeRequest(wire);
    ASSERT_EQ(decoded.status, KV::DecodeStatus::kComplete);
    EXPECT_EQ(decoded.command.name, "GET");
    ASSERT_EQ(decoded.command.arguments.size(), 1U);
    EXPECT_EQ(decoded.command.arguments[0], "key");
}

TEST(ProtocolTesting, PreservesCrLfInBulkPayload)
{
    KV::Protocol protocol;
    const KV::Command command = MakeCommand(std::pmr::get_default_resource(), "SET", {"key", "line1\r\nline2"});

    const KV::RequestDecode decoded = protocol.DecodeRequest(protocol.EncodeRequest(command));

    ASSERT_EQ(decoded.status, KV::DecodeStatus::kComplete);
    EXPECT_EQ(decoded.command.arguments[1], "line1\r\nline2");
}

TEST(ProtocolTesting, EncodesAndDecodesResponseArray)
{
    auto* resource = std::pmr::get_default_resource();
    KV::Result response {.type = KV::ResultType::kArray,
        .value = std::pmr::string(resource),
        .elements = std::pmr::vector<KV::Result>(resource)};
    response.elements.push_back(MakeResult(resource, KV::ResultType::kSimpleString, "OK"));
    response.elements.push_back(MakeResult(resource, KV::ResultType::kBulkString, "value\r\nwith newline"));
    response.elements.push_back(MakeResult(resource, KV::ResultType::kError, "key not found"));

    KV::Protocol protocol(resource);
    const std::pmr::string wire = protocol.EncodeResponse(response);
    EXPECT_EQ(std::string_view(wire), "*3\r\n+OK\r\n$19\r\nvalue\r\nwith newline\r\n-ERR key not found\r\n");

    const KV::ResponseDecode decoded = protocol.DecodeResponse(wire);
    ASSERT_EQ(decoded.status, KV::DecodeStatus::kComplete);
    ASSERT_EQ(decoded.result.elements.size(), 3U);
    EXPECT_EQ(decoded.result.elements[1].value, "value\r\nwith newline");
    EXPECT_EQ(decoded.result.elements[2].type, KV::ResultType::kError);
}

TEST(ProtocolTesting, SessionQueuesCommandsUntilExec)
{
    TestSession session;
    const KV::Command multi = MakeCommand(std::pmr::get_default_resource(), "MULTI");
    const KV::Command set = MakeCommand(std::pmr::get_default_resource(), "SET", {"key", "value"});
    const KV::Command get = MakeCommand(std::pmr::get_default_resource(), "GET", {"key"});
    const KV::Command exec = MakeCommand(std::pmr::get_default_resource(), "EXEC");
    std::size_t executed = 0;
    const auto execute = [&executed](const KV::Command& command) {
        ++executed;
        return MakeResult(std::pmr::get_default_resource(), KV::ResultType::kSimpleString, command.name);
    };

    EXPECT_EQ(session.Process(multi, execute).value, "OK");
    EXPECT_EQ(session.Process(set, execute).value, "QUEUED");
    EXPECT_EQ(session.Process(get, execute).value, "QUEUED");
    EXPECT_EQ(executed, 0U);

    const KV::Result result = session.Process(exec, execute);
    ASSERT_EQ(result.type, KV::ResultType::kArray);
    ASSERT_EQ(result.elements.size(), 2U);
    EXPECT_EQ(result.elements[0].value, "SET");
    EXPECT_EQ(result.elements[1].value, "GET");
    EXPECT_EQ(executed, 2U);
}
