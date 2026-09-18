#include <gtest/gtest.h>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <algorithm>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <Application/Server/AppendOnlyFile.hpp>
#include <Application/Server/Store.hpp>
#include <Foundation/Async/Async.hpp>

namespace
{
using StoreType = KV::LRUStore<std::string, std::string, KV::HashMap>;

std::string random_text(std::mt19937_64 &rng, std::size_t min_size, std::size_t max_size)
{
    static constexpr char alphabet[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<std::size_t> size_dist(min_size, max_size);
    std::uniform_int_distribution<std::size_t> char_dist(0, sizeof(alphabet) - 2);

    std::string value;
    const auto count = size_dist(rng);
    value.reserve(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        value.push_back(alphabet[char_dist(rng)]);
    }
    return value;
}

std::map<std::string, std::string> collect(StoreType &store)
{
    std::map<std::string, std::string> entries;
    store.visit_live([&](const std::string &key, const std::string &value, const auto &) {
        entries.emplace(key, value);
    });
    return entries;
}

KV::Command make_set(std::string key, std::string value)
{
    return {.type = KV::CommandType::kSet,
            .parameters = KV::SetParams{.key = std::move(key), .value = std::move(value)}};
}

KV::Command make_del(std::string key)
{
    return {.type = KV::CommandType::kDel, .parameters = KV::DelParams{.key = std::move(key)}};
}

// One writer's share of the load: entries of its own, appended one after the
// other, into a file the other writers are appending to as well.
Foundation::NBIO::Task<void> append_entries(std::shared_ptr<KV::AppendOnlyFile> aof, std::size_t writer,
                                            std::size_t count)
{
    for (std::size_t index = 0; index < count; ++index)
    {
        const std::string suffix = std::to_string(writer) + ":" + std::to_string(index);
        co_await aof->append(make_set("writer" + suffix, "value" + suffix));
    }
}

char value_byte(std::size_t writer, std::size_t index)
{
    return static_cast<char>('a' + ((writer * 7 + index) % 26));
}

// The same, with values large enough that the entry cannot be encoded into one
// small buffer: an entry is the unit the log is made of, so it still has to reach
// the file in one piece.
Foundation::NBIO::Task<void> append_large_entries(std::shared_ptr<KV::AppendOnlyFile> aof, std::size_t writer,
                                                 std::size_t count, std::size_t value_bytes)
{
    for (std::size_t index = 0; index < count; ++index)
    {
        const std::string suffix = std::to_string(writer) + ":" + std::to_string(index);
        co_await aof->append(make_set("large:" + suffix, std::string(value_bytes, value_byte(writer, index))));
    }
}
// Many entries, one log, appended at the same time -- one writer per client
// session, which is what a busy server looks like from the log's side.
void many_appends_one_log(const char *prefix)
{
    const auto temp_path = std::filesystem::temp_directory_path() /
                           (std::string{prefix} + std::to_string(std::random_device{}()) + ".aof");

    constexpr std::size_t kWriters = 16;
    constexpr std::size_t kEntriesPerWriter = 64;

    bool enabled = false;
    Foundation::NBIO::run([&]() -> Foundation::NBIO::Task<void> {
        auto aof = std::make_shared<KV::AppendOnlyFile>(temp_path);
        enabled = aof->enable();
        if (!enabled)
        {
            co_return;
        }

        std::vector<Foundation::Async::CoroutineToken> writers;
        writers.reserve(kWriters);
        for (std::size_t writer = 0; writer < kWriters; ++writer)
        {
            writers.push_back(Foundation::NBIO::spawn(append_entries(aof, writer, kEntriesPerWriter)));
        }
        // The test is not finished until every writer is. A writer whose only
        // reference was dropped by an overlap never runs again, so its join is
        // what turns that into a failing (hanging) test instead of a log that
        // simply has fewer entries in it.
        for (const Foundation::Async::CoroutineToken &writer : writers)
        {
            co_await writer;
        }
        co_return;
    }());

    ASSERT_TRUE(enabled);

    KV::AppendOnlyFile aof(temp_path);
    std::size_t replayed = 0;
    std::map<std::string, std::string> entries;

    // Replaying is the check on the file: it only gets to the end if every byte
    // of it is a whole command, so two appends that interleaved inside one entry
    // fail right here.
    ASSERT_TRUE(aof.replay([&](const KV::Command &command) {
        const auto *parameters = std::get_if<KV::SetParams>(&command.parameters);
        if (parameters == nullptr)
        {
            return false;
        }
        ++replayed;
        entries.insert_or_assign(parameters->key, parameters->value);
        return true;
    }));

    EXPECT_EQ(replayed, kWriters * kEntriesPerWriter);
    EXPECT_EQ(entries.size(), kWriters * kEntriesPerWriter);
    for (std::size_t writer = 0; writer < kWriters; ++writer)
    {
        for (std::size_t index = 0; index < kEntriesPerWriter; ++index)
        {
            const std::string suffix = std::to_string(writer) + ":" + std::to_string(index);
            const auto entry = entries.find("writer" + suffix);
            ASSERT_NE(entry, entries.end()) << "writer" << suffix;
            EXPECT_EQ(entry->second, "value" + suffix);
        }
    }

    std::filesystem::remove(temp_path);
}
} // namespace

TEST(AppendOnlyFileTesting, SaveToggleAndReplay)
{
    const auto temp_path = std::filesystem::temp_directory_path() /
                           ("kvstore-aof-" + std::to_string(std::random_device{}()) + ".aof");

    std::mt19937_64 rng{0x616f662d7265706cULL};
    StoreType source;
    std::map<std::string, std::string> expected;

    bool ok = false;
    Foundation::NBIO::run([&]() -> Foundation::NBIO::Task<void> {
        KV::AppendOnlyFile aof(temp_path);
        ok = aof.enable();
        if (!ok)
        {
            co_return;
        }

        for (std::size_t index = 0; index < 16; ++index)
        {
            const std::string key = "key_" + std::to_string(index) + "_" + random_text(rng, 4, 10);
            const std::string value = random_text(rng, 8, 24);
            source.set(key, value);
            expected.insert_or_assign(key, value);
            co_await aof.append(make_set(key, value));
        }

        const std::string removed_key = expected.begin()->first;
        source.set(removed_key, std::nullopt);
        expected.erase(removed_key);
        co_await aof.append(make_del(removed_key));

        aof.disable();
        co_await aof.append(make_set("disabled-key", "disabled-value"));

        ok = aof.enable();
        if (!ok)
        {
            co_return;
        }

        const std::string extra_key = "extra_" + random_text(rng, 4, 10);
        const std::string extra_value = random_text(rng, 8, 24);
        source.set(extra_key, extra_value);
        expected.insert_or_assign(extra_key, extra_value);
        co_await aof.append(make_set(extra_key, extra_value));
        aof.disable();
        co_return;
    }());

    ASSERT_TRUE(ok);

    KV::AppendOnlyFile aof(temp_path);
    ASSERT_TRUE(aof.enable());

    StoreType restored;
    ASSERT_TRUE(aof.replay([&](const KV::Command &command) {
        switch (command.type)
        {
        case KV::CommandType::kSet: {
            const auto &set = std::get<KV::SetParams>(command.parameters);
            restored.set(set.key, set.value);
            return true;
        }
        case KV::CommandType::kDel: {
            const auto &del = std::get<KV::DelParams>(command.parameters);
            restored.set(del.key, std::nullopt);
            return true;
        }
        default:
            return false;
        }
    }));

    EXPECT_EQ(expected, collect(restored));

    std::filesystem::remove(temp_path);
}

// Every client session appends to the same file, so on a busy server a dozen
// appends are in flight at once. The file drives one write job on one channel,
// and the offset a write lands at is read when that job is armed: an append that
// parks while another is parked writes over its job and drops its coroutine -- a
// session that never answers again, a control block the scheduler never
// reclaims (the 'control block leaked' assertion), and a log left holding half
// of one command followed by another.
TEST(AppendOnlyFileTesting, ConcurrentAppendsKeepTheLogWhole)
{
    many_appends_one_log("kvstore-aof-concurrent-");
}

// The completion backend is the one the server runs on, and it carries the same
// load: several entries are submitted together, and one completion can finish
// more than one of them.
TEST(AppendOnlyFileTesting, ConcurrentAppendsKeepTheLogWholeOnURing)
{
    Foundation::NBIO::initialize(std::make_unique<Foundation::NBIO::URingMultiplexer>());
    many_appends_one_log("kvstore-aof-concurrent-uring-");
}

// A large value is the case that would be streamed into the file in instalments if
// the entry were encoded straight through the write. It is still one entry, and
// the log has to come back whole: an entry interleaved with another is a log with
// no way back.
TEST(AppendOnlyFileTesting, ConcurrentLargeEntriesKeepTheLogWhole)
{
    const auto temp_path = std::filesystem::temp_directory_path() /
                           ("kvstore-aof-large-" + std::to_string(std::random_device{}()) + ".aof");

    constexpr std::size_t kWriters = 8;
    constexpr std::size_t kEntriesPerWriter = 4;
    constexpr std::size_t kValueBytes = 256U * 1024U;

    bool enabled = false;
    Foundation::NBIO::run([&]() -> Foundation::NBIO::Task<void> {
        auto aof = std::make_shared<KV::AppendOnlyFile>(temp_path);
        enabled = aof->enable();
        if (!enabled)
        {
            co_return;
        }

        std::vector<Foundation::Async::CoroutineToken> writers;
        writers.reserve(kWriters);
        for (std::size_t writer = 0; writer < kWriters; ++writer)
        {
            writers.push_back(
                Foundation::NBIO::spawn(append_large_entries(aof, writer, kEntriesPerWriter, kValueBytes)));
        }
        for (const Foundation::Async::CoroutineToken &writer : writers)
        {
            co_await writer;
        }
        co_return;
    }());

    ASSERT_TRUE(enabled);

    KV::AppendOnlyFile aof(temp_path);
    std::size_t replayed = 0;
    bool whole = true;
    ASSERT_TRUE(aof.replay([&](const KV::Command &command) {
        const auto *parameters = std::get_if<KV::SetParams>(&command.parameters);
        if (parameters == nullptr)
        {
            return false;
        }
        ++replayed;

        // The key says which entry this is meant to be, and the value says whether
        // every byte of it is there and whether any of it belongs to another entry.
        const auto first = parameters->key.find(':');
        const auto second = first == std::string::npos ? std::string::npos : parameters->key.find(':', first + 1);
        if (second == std::string::npos)
        {
            whole = false;
            return false;
        }
        const std::size_t writer = std::stoul(parameters->key.substr(first + 1, second - first - 1));
        const std::size_t index = std::stoul(parameters->key.substr(second + 1));
        if (writer >= kWriters || index >= kEntriesPerWriter || parameters->value.size() != kValueBytes)
        {
            whole = false;
            return false;
        }
        const char expected = value_byte(writer, index);
        whole = whole && std::all_of(parameters->value.begin(), parameters->value.end(),
                                     [expected](char byte) { return byte == expected; });
        return true;
    }));

    EXPECT_TRUE(whole) << "an entry was mixed with another";
    EXPECT_EQ(replayed, kWriters * kEntriesPerWriter);

    std::filesystem::remove(temp_path);
}
