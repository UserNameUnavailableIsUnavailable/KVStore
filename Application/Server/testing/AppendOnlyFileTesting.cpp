#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <utility>

#include <Application/Server/AppendOnlyFile.hpp>
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

KV::Command make_set(std::string key, std::string value)
{
    return {.type = KV::CommandType::kSet,
            .parameters = KV::SetParams{.key = std::move(key), .value = std::move(value)}};
}

KV::Command make_del(std::string key)
{
    return {.type = KV::CommandType::kDel, .parameters = KV::DelParams{.key = std::move(key)}};
}
} // namespace

TEST(AppendOnlyFileTesting, SaveToggleAndReplay)
{
    const auto temp_path = std::filesystem::temp_directory_path() /
                           ("kvstore-aof-" + std::to_string(std::random_device{}()) + ".aof");

    KV::AppendOnlyFile aof(temp_path);
    ASSERT_TRUE(aof.enable());

    std::mt19937_64 rng{0x616f662d7265706cULL};
    StoreType source;
    std::map<std::string, std::string> expected;

    for (std::size_t index = 0; index < 16; ++index)
    {
        const std::string key = "key_" + std::to_string(index) + "_" + random_text(rng, 4, 10);
        const std::string value = random_text(rng, 8, 24);
        source.set(key, value);
        expected.insert_or_assign(key, value);
        ASSERT_TRUE(aof.append(make_set(key, value)));
    }

    const std::string removed_key = expected.begin()->first;
    source.set(removed_key, std::nullopt);
    expected.erase(removed_key);
    ASSERT_TRUE(aof.append(make_del(removed_key)));

    aof.disable();
    ASSERT_TRUE(aof.append(make_set("disabled-key", "disabled-value")));

    ASSERT_TRUE(aof.enable());
    const std::string extra_key = "extra_" + random_text(rng, 4, 10);
    const std::string extra_value = random_text(rng, 8, 24);
    source.set(extra_key, extra_value);
    expected.insert_or_assign(extra_key, extra_value);
    ASSERT_TRUE(aof.append(make_set(extra_key, extra_value)));

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
