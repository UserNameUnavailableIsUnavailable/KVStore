#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <random>
#include <string>

#include <Foundation/Async/Async.hpp>

#include <Application/Server/Backup.hpp>
#include <Application/Server/Store.hpp>

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
} // namespace

TEST(BackupTesting, SaveAndLoadRoundTrip)
{
    const auto temp_path = std::filesystem::temp_directory_path() /
                           ("kvstore-backup-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".rdb");

    StoreType source;
    std::map<std::string, std::string> expected;
    std::mt19937_64 rng{0x6B657976616C7565ULL};
    for (std::size_t index = 0; index < 32; ++index)
    {
        const std::string key = "key_" + std::to_string(index) + "_" + random_text(rng, 4, 12);
        const std::string value = random_text(rng, 8, 32);
        source.set(key, value);
        expected.insert_or_assign(key, value);
    }

    KV::Backup backup(temp_path);
    bool saved = false;
    Foundation::Async::run([&]() -> Foundation::Async::Task<void> {
        saved = co_await backup.save(source);
        co_return;
    }());
    ASSERT_TRUE(saved);
    ASSERT_TRUE(std::filesystem::exists(temp_path));

    StoreType restored;
    ASSERT_TRUE(backup.load(restored));

    EXPECT_EQ(expected, collect(restored));

    std::filesystem::remove(temp_path);
}