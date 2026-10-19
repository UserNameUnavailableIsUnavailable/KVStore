#include "../replay_backlog.hpp"
#include <replication/replication_daemon.hpp>
#include <nbio/core/epoll_multiplexer.hpp>
#include <nbio/core/uring_multiplexer.hpp>
#include <nbio/net/tcp_connect_service.hpp>
#include <nbio/time.hpp>
#include <gtest/gtest.h>
#include <array>
#include <stdexcept>
#include <thread>

namespace {
using Session = nbio::net::TcpSessionService;

void Heartbeat(replication::ReplayBacklog& state, std::uint64_t sequence) {
    state.Consume({7, sequence, true, {}});
}

void Record(replication::ReplayBacklog& state, std::string_view bytes) {
    const auto end = state.head + bytes.size();
    state.Consume({7, end, false, {{end, std::string(bytes)}}});
}

nbio::async::Task<void> Send(Session& session, std::string bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        auto count = co_await session.Send(std::span<const char>(bytes.data() + offset, bytes.size() - offset));
        if (!count || *count == 0) throw std::runtime_error("test send failed");
        offset += *count;
    }
}

nbio::async::Task<std::string> Read(Session& session, std::size_t length) {
    std::string bytes(length, '\0');
    std::size_t offset = 0;
    while (offset < length) {
        auto count = co_await session.Receive(std::span<char>(bytes.data() + offset, length - offset));
        if (!count || *count == 0) throw std::runtime_error("test stream ended early");
        offset += *count;
    }
    co_return bytes;
}

nbio::async::Task<void> Closed(Session& session) {
    char byte;
    const auto count = co_await session.Receive(std::span<char>(&byte, 1));
    EXPECT_TRUE(!count || *count == 0);
}

nbio::async::Task<std::shared_ptr<Session>> Connect(nbio::net::Address address) {
    auto result = co_await nbio::net::TcpConnectService{}.Connect(address);
    if (!result) throw std::system_error(result.error(), "test connect failed");
    co_return *result;
}

nbio::async::Task<void> Exercise(
    nbio::net::Address address, replication::ReplayBacklog& state, replication::ReplicationDaemon& daemon,
    replication::ReplicationInfo& info) {
    const auto first_ticket = info.AttachReplica();
    auto session = co_await Connect(address);
    co_await Send(*session, "FOLLOW 7 ");
    co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(10));
    co_await Send(*session, "10 " + std::to_string(first_ticket) + "\r\n");
    EXPECT_EQ(co_await Read(*session, 13), "+CONTINUE 7\r\n");
    EXPECT_EQ(info.replica_count.load(), 1);
    EXPECT_FALSE(info.CancelReplica(first_ticket));

    const std::string binary("a\0b\r\n", 5);
    Record(state, binary);
    daemon.Maintain();
    const auto frame = "@7 15 5\r\n" + binary;
    EXPECT_EQ(co_await Read(*session, frame.size()), frame);
    auto second = co_await Connect(address);
    const auto second_ticket = info.AttachReplica();
    co_await Send(*second, "FOLLOW 7 15 " + std::to_string(second_ticket) + "\r\n");
    EXPECT_EQ(co_await Read(*second, 13), "+CONTINUE 7\r\n");
    EXPECT_EQ(info.replica_count.load(), 2);
    auto duplicate = co_await Connect(address);
    co_await Send(*duplicate, "FOLLOW 7 15 " + std::to_string(second_ticket) + "\r\n");
    co_await Closed(*duplicate);
    EXPECT_EQ(info.replica_count.load(), 2);

    // Delay reads until the daemon has entered its asynchronous send path.
    std::string expected;
    for (std::uint64_t sequence = 12; sequence < 2060; ++sequence) {
        const std::string payload(4096, static_cast<char>('a' + sequence % 26));
        Record(state, payload);
        expected += "@7 " + std::to_string(state.head) + " 4096\r\n" + payload;
    }
    daemon.Maintain();
    co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(20));
    EXPECT_EQ(co_await Read(*session, expected.size()), expected);
    EXPECT_EQ(co_await Read(*second, expected.size()), expected);

    auto rejected = co_await Connect(address);
    const auto rejected_ticket = info.AttachReplica();
    co_await Send(*rejected, "FOLLOW 8 " + std::to_string(state.head) + " " +
                 std::to_string(rejected_ticket) + "\r\n");
    co_await Closed(*rejected);
    EXPECT_EQ(info.replica_count.load(), 3);
    EXPECT_TRUE(info.CancelReplica(rejected_ticket));
    auto malformed = co_await Connect(address);
    co_await Send(*malformed, "FOLLOW 7 2059\r\nunexpected");
    co_await Closed(*malformed);
    auto oversized = co_await Connect(address);
    co_await Send(*oversized, std::string(97, 'x'));
    co_await Closed(*oversized);

    // The application heartbeat must report exactly the applied stream head.
    co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(510));
    daemon.Maintain();
    const auto heartbeat = "@7 " + std::to_string(state.head) + " 0\r\n";
    EXPECT_EQ(co_await Read(*session, heartbeat.size()), heartbeat);
    EXPECT_EQ(co_await Read(*second, heartbeat.size()), heartbeat);

    state.Invalidate();
    Heartbeat(state, 2060);
    daemon.Maintain();
    co_await Closed(*session);
    co_await Closed(*second);
    EXPECT_EQ(info.replica_count.load(), 0);
    auto stale = co_await Connect(address);
    co_await Send(*stale, "FOLLOW 7 2059\r\n");
    co_await Closed(*stale);
    auto cancelled = co_await Connect(address);
    const auto cancelled_ticket = info.AttachReplica();
    EXPECT_TRUE(info.CancelReplica(cancelled_ticket));
    co_await Send(*cancelled, "FOLLOW 7 2060 " + std::to_string(cancelled_ticket) + "\r\n");
    co_await Closed(*cancelled);
    EXPECT_EQ(info.replica_count.load(), 0);
    auto current = co_await Connect(address);
    co_await Send(*current, "FOLLOW 7 2060 " + std::to_string(info.AttachReplica()) + "\r\n");
    EXPECT_EQ(co_await Read(*current, 13), "+CONTINUE 7\r\n");
    co_await Send(*current, "unexpected");
    co_await Closed(*current);
    EXPECT_EQ(info.replica_count.load(), 0);

    // Incomplete handshakes are expired by maintenance, not left parked forever.
    auto stalled = co_await Connect(address);
    co_await Send(*stalled, "FOLLOW ");
    co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(5100));
    daemon.Maintain();
    co_await Closed(*stalled);

    auto active = co_await Connect(address);
    co_await Send(*active, "FOLLOW 7 2060 " + std::to_string(info.AttachReplica()) + "\r\n");
    EXPECT_EQ(co_await Read(*active, 13), "+CONTINUE 7\r\n");
    auto pending = co_await Connect(address);
    co_await Send(*pending, "FOLLOW ");
    co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(10));
    daemon.Stop();
    EXPECT_EQ(info.replica_count.load(), 0);
    EXPECT_EQ(info.daemon_ready.load(), 0);
    co_await Closed(*active);
    co_await Closed(*pending);
}

nbio::async::Task<void> Maintain(replication::ReplicationDaemon& daemon) {
    while (true) {
        daemon.Maintain();
        co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::milliseconds(10));
    }
}

nbio::async::Task<void> Timeout() {
    co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::seconds(15));
    throw std::runtime_error("daemon transport test timed out");
}

nbio::async::Task<void> RunTransport() {
    // Bind port zero first to avoid assuming any particular port is available.
    auto local = nbio::net::Address::FromIPv4("127.0.0.1", 0);
    nbio::net::TcpListener reservation;
    const auto bound = reservation.Bind(*local);
    if (!bound) throw std::system_error(bound.error());
    const auto assigned = reservation.GetLocalAddress();
    if (!assigned) throw std::system_error(assigned.error());
    const auto address = *assigned;
    reservation.Close();

    replication::ReplayBacklog state(16U * 1024U * 1024U);
    Heartbeat(state, 10);
    auto mapping = replication::SharedReplicationInfo::Create();
    replication::ReplicationDaemon daemon(state, mapping.get());
    co_await nbio::async::WhenAny(
        daemon.Serve(address), Maintain(daemon), Exercise(address, state, daemon, mapping.get()), Timeout());
}

class DaemonTransportTest : public ::testing::TestWithParam<bool> {};

TEST_P(DaemonTransportTest, OrderedReplayRecoveryAndBackpressure) {
    std::exception_ptr failure;
    const bool uring = GetParam();
    std::thread worker([&failure, uring] {
        try {
            if (uring) {
                nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::URingMultiplexer>());
            } else {
                nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::EpollMultiplexer>());
            }
            nbio::async::Run(RunTransport());
        } catch (...) {
            failure = std::current_exception();
        }
    });
    worker.join();
    if (failure) std::rethrow_exception(failure);
}

INSTANTIATE_TEST_SUITE_P(Backends, DaemonTransportTest, ::testing::Values(false, true));
}  // namespace
