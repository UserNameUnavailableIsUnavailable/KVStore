#include "../replay_backlog.hpp"
#include <replication/stream_protocol.hpp>
#include <storage/append_only_file.hpp>
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
#include <nbio/core/epoll_multiplexer.hpp>
#include <nbio/async.hpp>
#include <thread>
#include <sys/wait.h>
#include <replication/repld_process.hpp>

namespace {
struct ObservedLog {
    std::uint64_t epoch;
    std::uint64_t offset;
    std::string payload;
};
std::vector<ObservedLog> observed;

nbio::async::Task<void> WriteAof(KV::AppendOnlyFile& log, const KV::Command& command) {
    auto payload = log.log(command);
    co_await log.append_encoded(payload);
}

nbio::async::Task<void> OverlapAof(KV::AppendOnlyFile& log, const KV::Command& first,
                                  const KV::Command& second) {
    nbio::async::Spawn(WriteAof(log, first));
    co_await WriteAof(log, second);
}

kv_log_event Chunk(std::uint64_t end, std::uint64_t length,
                   std::uint32_t position, std::string_view bytes) {
    kv_log_event event{};
    event.epoch = 7;
    event.end_offset = end;
    event.length = length;
    event.position = position;
    event.size = bytes.size();
    std::memcpy(event.data, bytes.data(), bytes.size());
    return event;
}
}

extern "C" void __wrap_kvstore_write_log_v1(
    std::uint64_t epoch, std::uint64_t offset, const char* payload, std::uint64_t length) noexcept {
    observed.push_back({epoch, offset, length ? std::string(payload, length) : std::string{}});
}

TEST(LogicalLog, DisabledAofStillPublishesCanonicalBytesAndOffsets) {
    observed.clear();
    const auto path = std::filesystem::temp_directory_path() /
                      ("kvstore-log-test-" + std::to_string(::getpid()) + ".aof");
    KV::AppendOnlyFile log(path);
    const KV::Command set{KV::CommandType::kSet, KV::SetParams{"key", std::string("a\0b", 3)}};
    const KV::Command del{KV::CommandType::kDel, KV::DelParams{"key"}};
    const auto epoch = log.epoch();
    EXPECT_TRUE(log.log(set).empty());
    EXPECT_TRUE(observed.empty());
    EXPECT_EQ(log.offset(), 0);
    log.activate_replication();
    const std::string first(log.log(set));
    EXPECT_EQ(first, KV::EncodeCommand(set));
    EXPECT_EQ(log.offset(), first.size());
    log.observe();
    const std::string second(log.log(del));
    EXPECT_EQ(log.offset(), first.size() + second.size());
    EXPECT_EQ(log.epoch(), epoch);
    EXPECT_FALSE(log.enabled());
    EXPECT_FALSE(std::filesystem::exists(path));
    ASSERT_EQ(observed.size(), 3);
    EXPECT_EQ(observed[0].epoch, epoch);
    EXPECT_EQ(observed[0].offset, first.size());
    EXPECT_EQ(observed[0].payload, first);
    EXPECT_EQ(observed[1].offset, first.size());
    EXPECT_TRUE(observed[1].payload.empty());
    EXPECT_EQ(observed[2].offset, first.size() + second.size());
    EXPECT_EQ(observed[2].payload, second);
}

TEST(LogicalLog, BufferIsReusedAndActivationSurvivesAofDisable) {
    observed.clear();
    KV::AppendOnlyFile log;
    const KV::Command set{KV::CommandType::kSet, KV::SetParams{"key", "value"}};
    EXPECT_FALSE(log.logging());
    EXPECT_TRUE(log.log(set).empty());
    log.activate_replication();
    const auto first = log.log(set);
    const auto* storage = first.data();
    const std::string expected(first);
    log.disable();
    EXPECT_TRUE(log.logging());
    const auto second = log.log(set);
    EXPECT_EQ(second.data(), storage);
    EXPECT_EQ(second, expected);
    EXPECT_EQ(log.offset(), expected.size() * 2);
    ASSERT_EQ(observed.size(), 2);
}

TEST(ReplicationInfo, MasterAttachesAndDaemonOnlyInheritsAndDetaches) {
    auto mapping = replication::SharedReplicationInfo::Create();
    auto peer = replication::SharedReplicationInfo::Map(mapping.descriptor());
    auto& master = mapping.get();
    auto& daemon = peer.get();
    const auto ticket = master.AttachReplica();
    EXPECT_EQ(daemon.replica_count.load(), 1);
    EXPECT_TRUE(daemon.ClaimReplica(ticket));
    EXPECT_EQ(master.replica_count.load(), 1);
    EXPECT_FALSE(master.CancelReplica(ticket));
    EXPECT_FALSE(daemon.ClaimReplica(ticket));
    EXPECT_TRUE(daemon.DetachReplica(ticket));
    EXPECT_FALSE(daemon.DetachReplica(ticket));
    EXPECT_EQ(master.replica_count.load(), 0);
    const auto failed = master.AttachReplica();
    EXPECT_FALSE(daemon.DetachReplica(failed));
    EXPECT_TRUE(master.CancelReplica(failed));
    EXPECT_FALSE(daemon.ClaimReplica(failed));
    EXPECT_EQ(master.replica_count.load(), 0);
}

TEST(ReplicationInfo, CapacityAndStaleTickets) {
    replication::ReplicationInfo info;
    std::vector<std::uint64_t> tickets;
    for (std::size_t i = 0; i < info.kMaxReplicas; ++i) tickets.push_back(info.AttachReplica());
    EXPECT_THROW(info.AttachReplica(), std::runtime_error);
    EXPECT_TRUE(info.CancelReplica(tickets[17]));
    const auto replacement = info.AttachReplica();
    EXPECT_NE(replacement, tickets[17]);
    EXPECT_FALSE(info.ClaimReplica(tickets[17]));
    EXPECT_FALSE(info.CancelReplica(tickets[17]));
    EXPECT_EQ(info.replica_count.load(), info.kMaxReplicas);
    EXPECT_TRUE(info.CancelReplica(replacement));
    for (const auto ticket : tickets) info.CancelReplica(ticket);
    EXPECT_EQ(info.replica_count.load(), 0);
}

TEST(ReplicationInfo, TicketExhaustionDoesNotAliasSlotReservation) {
    replication::ReplicationInfo info;
    info.next_ticket.store((1ULL << 63) - 1);
    const auto ticket = info.AttachReplica();
    EXPECT_TRUE(info.ClaimReplica(ticket));
    EXPECT_TRUE(info.DetachReplica(ticket));
    EXPECT_EQ(info.replica_count.load(), 0);
    EXPECT_THROW(info.AttachReplica(), std::overflow_error);
    EXPECT_FALSE(info.ClaimReplica(0));
    EXPECT_FALSE(info.CancelReplica(1ULL << 63));
    EXPECT_FALSE(info.DetachReplica(UINT64_MAX));
    EXPECT_EQ(info.replica_count.load(), 0);
}

TEST(ReplicationInfo, ClaimCancelRaceAcrossProcessesHasOneDecrementOwner) {
    auto mapping = replication::SharedReplicationInfo::Create();
    for (unsigned attempt = 0; attempt < 32; ++attempt) {
        const auto ticket = mapping.get().AttachReplica();
        const auto child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            auto peer = replication::SharedReplicationInfo::Map(mapping.descriptor());
            const bool claimed = peer.get().ClaimReplica(ticket);
            if (claimed && !peer.get().DetachReplica(ticket)) ::_exit(2);
            ::_exit(0);
        }
        mapping.get().CancelReplica(ticket);
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);
        EXPECT_EQ(mapping.get().replica_count.load(), 0);
    }
}

TEST(ReplicationInfo, StartupFailuresAreReportedAndChildrenAreReaped) {
    auto mapping = replication::SharedReplicationInfo::Create();
    EXPECT_THROW(replication::RepldProcess(mapping, "/no/such/repld", "127.0.0.1", 8081, "epoll"),
                 std::system_error);
    EXPECT_THROW(replication::RepldProcess(mapping, "/bin/false", "127.0.0.1", 8081, "epoll"),
                 std::runtime_error);
}

TEST(ReplicationInfo, ChildMapsInheritedDescriptorAndStopsWithOwner) {
    auto mapping = replication::SharedReplicationInfo::Create();
    {
        replication::RepldProcess child(mapping, REPLD_PROCESS_FIXTURE, "127.0.0.1", 8081, "epoll");
        EXPECT_EQ(mapping.get().daemon_ready.load(), 1);
        EXPECT_NO_THROW(child.Check());
    }
    EXPECT_EQ(mapping.get().daemon_ready.load(), 0);
}

TEST(ReplicationInfo, UnexpectedChildExitIsReportedEvenOnRepeatedChecks) {
    auto mapping = replication::SharedReplicationInfo::Create();
    replication::RepldProcess child(mapping, REPLD_PROCESS_FIXTURE, "127.0.0.1", 8081, "exit-after-ready");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_THROW(child.Check(), std::runtime_error);
    EXPECT_THROW(child.Check(), std::runtime_error);
}

TEST(ReplicationInfo, StartupTimeoutTerminatesUnreadyChild) {
    auto mapping = replication::SharedReplicationInfo::Create();
    const auto start = std::chrono::steady_clock::now();
    EXPECT_THROW(replication::RepldProcess(mapping, REPLD_PROCESS_FIXTURE, "127.0.0.1", 8081, "never-ready"),
                 std::runtime_error);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_GE(elapsed, std::chrono::seconds(5));
    EXPECT_LT(elapsed, std::chrono::seconds(7));
    EXPECT_EQ(mapping.get().daemon_ready.load(), 0);
}

TEST(LogicalLog, SharedCountGatesPayloadAndAofOverridesZeroReplicas) {
    observed.clear();
    auto mapping = replication::SharedReplicationInfo::Create();
    KV::AppendOnlyFile log;
    log.replication_info(&mapping.get());
    log.activate_replication();
    const KV::Command command{KV::CommandType::kSet, KV::SetParams{"key", "value"}};
    EXPECT_FALSE(log.logging());
    EXPECT_TRUE(log.log(command).empty());
    EXPECT_TRUE(observed.empty());
    const auto ticket = mapping.get().AttachReplica();
    EXPECT_TRUE(log.logging());
    const std::string payload(log.log(command));
    EXPECT_FALSE(payload.empty());
    EXPECT_TRUE(mapping.get().ClaimReplica(ticket));
    EXPECT_TRUE(log.logging());
    EXPECT_TRUE(mapping.get().DetachReplica(ticket));
    EXPECT_FALSE(log.logging());
    EXPECT_TRUE(log.log(command).empty());
    EXPECT_EQ(log.offset(), payload.size());
    const auto next = mapping.get().AttachReplica();
    EXPECT_EQ(log.log(command), payload);
    EXPECT_EQ(log.offset(), payload.size() * 2);
    EXPECT_TRUE(mapping.get().CancelReplica(next));
    const auto path = std::filesystem::temp_directory_path() /
        ("kvstore-count-aof-" + std::to_string(::getpid()) + ".aof");
    KV::AppendOnlyFile persistent(path);
    persistent.replication_info(&mapping.get());
    ASSERT_TRUE(persistent.enable());
    EXPECT_TRUE(persistent.logging());
    EXPECT_EQ(persistent.log(command), payload);
    persistent.disable();
    EXPECT_FALSE(persistent.logging());
    std::filesystem::remove(path);
}

TEST(LogicalLog, AofReusesBytesAndChecksumDoesNotChangeLogicalOffset) {
    const KV::Command command{KV::CommandType::kSet, KV::SetParams{"key", std::string(70000, 'x')}};
    const auto canonical = KV::EncodeCommand(command);
    for (const bool checksum : {false, true}) {
        observed.clear();
        const auto path = std::filesystem::temp_directory_path() /
            ("kvstore-log-aof-" + std::to_string(::getpid()) + (checksum ? "-crc" : "") + ".aof");
        std::exception_ptr failure;
        std::thread worker([&] {
            try {
                nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::EpollMultiplexer>());
                KV::AppendOnlyFile log(path);
                log.checksum(checksum);
                if (!log.enable()) throw std::runtime_error("cannot enable test AOF");
                nbio::async::Run(WriteAof(log, command));
                EXPECT_EQ(log.offset(), canonical.size());
                log.disable();
                int replayed = 0;
                EXPECT_TRUE(log.replay([&](const KV::Command& entry) {
                    EXPECT_EQ(KV::EncodeCommand(entry), canonical);
                    ++replayed;
                    return true;
                }));
                EXPECT_EQ(replayed, 1);
            } catch (...) { failure = std::current_exception(); }
        });
        worker.join();
        if (failure) std::rethrow_exception(failure);
        ASSERT_EQ(observed.size(), 1);
        EXPECT_EQ(observed.front().payload, canonical);
        EXPECT_EQ(observed.front().offset, canonical.size());
        EXPECT_EQ(std::filesystem::file_size(path), canonical.size() + (checksum ? 10 : 0));
        std::filesystem::remove(path);
    }
}

TEST(LogicalLog, SuspendedAofWriteDoesNotBorrowReusableEncodingBuffer) {
    observed.clear();
    const KV::Command first{KV::CommandType::kSet, KV::SetParams{"first", std::string(70000, 'a')}};
    const KV::Command second{KV::CommandType::kSet, KV::SetParams{"second", std::string(70000, 'b')}};
    const auto path = std::filesystem::temp_directory_path() /
        ("kvstore-overlap-aof-" + std::to_string(::getpid()) + ".aof");
    std::exception_ptr failure;
    std::thread worker([&] {
        try {
            nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::EpollMultiplexer>());
            KV::AppendOnlyFile log(path);
            if (!log.enable()) throw std::runtime_error("cannot enable test AOF");
            nbio::async::Run(OverlapAof(log, first, second));
            log.disable();
            int first_count = 0;
            int second_count = 0;
            EXPECT_TRUE(log.replay([&](const KV::Command& entry) {
                const auto encoded = KV::EncodeCommand(entry);
                if (encoded == KV::EncodeCommand(first)) ++first_count;
                else if (encoded == KV::EncodeCommand(second)) ++second_count;
                else return false;
                return true;
            }));
            EXPECT_EQ(first_count, 1);
            EXPECT_EQ(second_count, 1);
        } catch (...) { failure = std::current_exception(); }
    });
    worker.join();
    std::filesystem::remove(path);
    if (failure) std::rethrow_exception(failure);
    EXPECT_EQ(observed.size(), 2);
}

TEST(CaptureStream, BinaryChunkAssemblyAndOffsets) {
    replication::CaptureStream capture;
    EXPECT_TRUE(capture.Reset(7, 100).reset);
    const std::string bytes("a\0b\r\n", 5);
    auto batch = capture.Consume(Chunk(105, 5, 0, std::string_view(bytes).substr(0, 2)));
    EXPECT_TRUE(batch.records.empty());
    EXPECT_EQ(batch.head, 100);
    batch = capture.Consume(Chunk(105, 5, 2, std::string_view(bytes).substr(2)));
    ASSERT_EQ(batch.records.size(), 1);
    EXPECT_EQ(batch.records.front().payload, bytes);
    EXPECT_EQ(batch.head, 105);
    EXPECT_TRUE(capture.Consume(Chunk(105, 5, 0, bytes)).records.empty());
}

TEST(CaptureStream, GapsAndMalformedChunksAreExplicit) {
    replication::CaptureStream capture;
    capture.Reset(7, 100);
    EXPECT_THROW(capture.Consume(Chunk(106, 5, 0, "hello")), std::runtime_error);
    EXPECT_THROW(capture.Consume(Chunk(105, 5, 2, "llo")), std::runtime_error);
    auto event = Chunk(105, 5, 0, "hello");
    event.epoch = 8;
    EXPECT_THROW(capture.Consume(event), std::runtime_error);
    event = Chunk(105, 5, 0, "hello");
    event.size = KV_LOG_CHUNK + 1;
    EXPECT_THROW(capture.Consume(event), std::runtime_error);
}

TEST(CaptureStream, LossResetDiscardsPartialAndPreResetRecords) {
    replication::CaptureStream capture;
    capture.Reset(7, 0);
    capture.Consume(Chunk(5, 5, 0, "he"));
    capture.Reset(7, 5);
    EXPECT_TRUE(capture.Consume(Chunk(5, 5, 2, "llo")).records.empty());
    const auto batch = capture.Consume(Chunk(8, 3, 0, "new"));
    ASSERT_EQ(batch.records.size(), 1);
    EXPECT_EQ(batch.records.front().payload, "new");
}

TEST(CaptureStream, LostFinalRecordIsDetectedWithoutAnotherRingEvent) {
    replication::CaptureStream capture;
    kv_log_state status{};
    status.epoch = 7;
    status.head = 100;
    ASSERT_TRUE(capture.Observe(status));
    EXPECT_FALSE(capture.Observe(status));
    const auto batch = capture.Consume(Chunk(105, 5, 0, "hello"));
    ASSERT_EQ(batch.head, 105);
    // The next write never reaches the ring; only the status map advances.
    status.head = 110;
    status.failures = 1;
    const auto reset = capture.Observe(status);
    ASSERT_TRUE(reset);
    EXPECT_TRUE(reset->reset);
    EXPECT_EQ(reset->head, 110);
    EXPECT_FALSE(capture.Observe(status));
    EXPECT_TRUE(capture.Consume(Chunk(110, 5, 0, "stale")).records.empty());
    EXPECT_EQ(capture.Consume(Chunk(113, 3, 0, "new")).head, 113);
}

TEST(ReplayBacklog, ByteBoundariesRetentionAndReset) {
    replication::ReplayBacklog backlog(256);
    backlog.Consume({7, 0, true, {}});
    backlog.Consume({7, 9, false, {{3, "aaa"}, {6, "bbb"}, {9, "ccc"}}});
    EXPECT_FALSE(backlog.CanFollow(7, 0));
    EXPECT_TRUE(backlog.CanFollow(7, 6));
    EXPECT_FALSE(backlog.CanFollow(7, 7));
    const auto generation = backlog.generation;
    backlog.Consume({7, 20, true, {}});
    EXPECT_GT(backlog.generation, generation);
    EXPECT_TRUE(backlog.CanFollow(7, 20));
    EXPECT_FALSE(backlog.CanFollow(7, 9));
}

TEST(ReplayBacklog, GapsInvalidateAndOversizedRecordsRebase) {
    replication::ReplayBacklog backlog(256);
    backlog.Consume({7, 0, true, {}});
    backlog.Consume({7, 2, false, {{2, "x"}}});
    EXPECT_FALSE(backlog.healthy);
    backlog.Consume({7, 0, true, {}});
    backlog.Consume({7, 260, false, {{256, std::string(256, 'x')}, {260, "fits"}}});
    EXPECT_FALSE(backlog.CanFollow(7, 0));
    EXPECT_TRUE(backlog.CanFollow(7, 256));
    ASSERT_NE(backlog.Next(256), nullptr);
    EXPECT_EQ(backlog.Next(256)->payload, "fits");
}

TEST(ReplicationProtocol, ByteOffsetsCannotSkipOrOverflow) {
    const auto valid = KV::Replication::ParseFrame("@7 112 12");
    ASSERT_TRUE(valid);
    EXPECT_TRUE(KV::Replication::Follows(*valid, 7, 100));
    EXPECT_FALSE(KV::Replication::Follows(*valid, 7, 101));
    EXPECT_FALSE(KV::Replication::Follows(*valid, 8, 100));
    EXPECT_FALSE(KV::Replication::Follows({7, 0, 2}, 7, UINT64_MAX - 1));
    EXPECT_FALSE(KV::Replication::ParseFrame("@7 11 16777217"));
    EXPECT_FALSE(KV::Replication::ParseFrame("@7 11 -1"));
    EXPECT_TRUE(KV::Replication::Follows({7, 100, 0}, 7, 100));
}
