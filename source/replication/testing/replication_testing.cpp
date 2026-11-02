#include <replication/command_backlog.hpp>
#include <replication/stream_protocol.hpp>
#include <storage/append_only_file.hpp>

#include <commands.hpp>
#include <gtest/gtest.h>
#include <filesystem>
#include <string>
#include <unistd.h>

namespace {
struct ObservedLog {
    std::uint64_t epoch;
    std::uint64_t offset;
    std::string payload;
};
std::vector<ObservedLog> observed;
}  // namespace

extern "C" void __wrap_kvstore_write_log_v1(
    std::uint64_t epoch, std::uint64_t offset, const char* payload, std::uint64_t length) noexcept {
    observed.push_back({epoch, offset, length ? std::string(payload, length) : std::string{}});
}

TEST(CommandBacklog, RetentionAndConsecutiveOffsets) {
    replication::CommandBacklog backlog(2 * (sizeof(replication::CommandBacklog::Record) + 3));
    backlog.Reset(10);
    EXPECT_TRUE(backlog.Contains(10));
    EXPECT_FALSE(backlog.Contains(9));
    backlog.Append(11, "one");
    backlog.Append(12, "two");
    EXPECT_EQ(backlog.Next(10)->payload, "one");
    backlog.Append(13, "end");
    EXPECT_FALSE(backlog.Contains(10));
    EXPECT_EQ(backlog.Next(11)->offset, 12);
    EXPECT_TRUE(backlog.Contains(13));
    EXPECT_EQ(backlog.Next(13), nullptr);
    EXPECT_FALSE(backlog.Contains(14));
    EXPECT_THROW(backlog.Append(15, "gap"), std::runtime_error);
    backlog.Append(14, std::string(200, 'x'));
    EXPECT_FALSE(backlog.Contains(13));
    EXPECT_TRUE(backlog.Contains(14));
}

TEST(CommandBacklog, ResetRebasesTheWindowAtTheSnapshot) {
    replication::CommandBacklog backlog(64 * 1024);
    backlog.Reset(5);
    backlog.Append(6, "a");
    backlog.Append(7, "b");
    EXPECT_EQ(backlog.head(), 7u);
    // A fresh snapshot at 7 keeps 7 as the resumable point and drops older history.
    backlog.Reset(7);
    EXPECT_EQ(backlog.head(), 7u);
    EXPECT_TRUE(backlog.Contains(7));
    EXPECT_EQ(backlog.Next(7), nullptr);
    backlog.Append(8, "c");
    EXPECT_EQ(backlog.Next(7)->payload, "c");
}

TEST(StreamProtocol, FramesFollowConsecutiveOffsets) {
    const auto frame = KV::Replication::ParseFrame("@7 101 3");
    ASSERT_TRUE(frame.has_value());
    EXPECT_EQ(frame->epoch, 7u);
    EXPECT_EQ(frame->end_offset, 101u);
    EXPECT_EQ(frame->length, 3u);
    EXPECT_TRUE(KV::Replication::Follows(*frame, 7, 100));
    EXPECT_FALSE(KV::Replication::Follows(*frame, 7, 101));
    EXPECT_FALSE(KV::Replication::Follows(*frame, 8, 100));

    // A heartbeat is a length-0 frame at the current offset.
    const auto heartbeat = KV::Replication::ParseFrame("@7 100 0");
    ASSERT_TRUE(heartbeat.has_value());
    EXPECT_TRUE(KV::Replication::Follows(*heartbeat, 7, 100));
    EXPECT_FALSE(KV::Replication::Follows(*heartbeat, 7, 99));

    EXPECT_FALSE(KV::Replication::ParseFrame("not a frame").has_value());
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
    EXPECT_EQ(log.offset(), 1u);
    log.activate_replication();
    const std::string first(log.log(set));
    EXPECT_EQ(first, KV::EncodeCommand(set));
    EXPECT_EQ(log.offset(), 2u);
    log.observe();
    const std::string second(log.log(del));
    EXPECT_EQ(log.offset(), 3u);
    EXPECT_EQ(log.epoch(), epoch);
    EXPECT_FALSE(log.enabled());
    EXPECT_FALSE(std::filesystem::exists(path));
    ASSERT_EQ(observed.size(), 3u);
    EXPECT_EQ(observed[0].epoch, epoch);
    EXPECT_EQ(observed[0].offset, 2u);
    EXPECT_EQ(observed[0].payload, first);
    EXPECT_EQ(observed[1].offset, 2u);
    EXPECT_TRUE(observed[1].payload.empty());
    EXPECT_EQ(observed[2].offset, 3u);
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
    EXPECT_EQ(log.offset(), 3u);
    ASSERT_EQ(observed.size(), 2u);
}
