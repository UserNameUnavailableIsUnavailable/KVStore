#include <gtest/gtest.h>

#include <Application/Server/ReplicationService.hpp>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

using KV::ReplicationService;

namespace
{
// The options a master would be built with: `port` 0 leaves the listener off.
ReplicationService::Options ListenOn(std::string address, std::uint16_t port)
{
    ReplicationService::Options options;
    options.listen_address = std::move(address);
    options.listen_port = port;
    return options;
}
} // namespace

TEST(ReplicationServiceTesting, AWildcardAddressIsRefusedForTheReplicationPort)
{
    // A wildcard is accepted by rdma_bind_addr but names no device, so the
    // listener would die on the first thing that needs one -- a protection domain
    // -- rather than here, where the mistake is.
    EXPECT_THROW(ReplicationService(ListenOn("0.0.0.0", 8081), {}), std::invalid_argument);
    EXPECT_THROW(ReplicationService(ListenOn("", 8081), {}), std::invalid_argument);
    EXPECT_THROW(ReplicationService(ListenOn("::", 8081), {}), std::invalid_argument);
}

TEST(ReplicationServiceTesting, ADeviceAddressIsWhatTheListenerNeeds)
{
    // Building the service only decides; no device is touched until the listener
    // is served, so this needs no RDMA device to run.
    EXPECT_NO_THROW(ReplicationService(ListenOn("192.168.0.201", 8081), {}));
}

TEST(ReplicationServiceTesting, WithoutAReplicationPortTheAddressDoesNotMatter)
{
    EXPECT_NO_THROW(ReplicationService(ListenOn("0.0.0.0", 0), {}));
}

TEST(ReplicationServiceTesting, AnAddressIsNotWhereThePortGoes)
{
    // `192.168.0.201:8081` where an address belongs: the port has an option of
    // its own, and a listener that is handed the two together would bind none --
    // so this is refused even when no listener is being started.
    EXPECT_THROW(ReplicationService(ListenOn("192.168.0.201:8081", 8081), {}), std::invalid_argument);
    EXPECT_THROW(ReplicationService(ListenOn("192.168.0.201:8081", 0), {}), std::invalid_argument);
    EXPECT_THROW(ReplicationService(ListenOn("::1", 8081), {}), std::invalid_argument);
}

TEST(ReplicationServiceTesting, ThePoolsHaveToCarryOneConnection)
{
    // A stream takes kSendChunks + kReceiveChunks chunks, so a pool smaller than
    // that could never admit one replica.
    ReplicationService::Options options = ListenOn("192.168.0.201", 8081);
    options.chunk_count = 1;
    EXPECT_THROW(ReplicationService(options, {}), std::invalid_argument);

    options.chunk_count = 2 * Foundation::Core::RDMA_Stream::kSendChunks;
    EXPECT_NO_THROW(ReplicationService(options, {}));
}
