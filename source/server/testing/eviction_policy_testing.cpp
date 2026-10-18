#include <gtest/gtest.h>

#include <commands.hpp>
#include <storage/store.hpp>

#include <array>
#include <span>
#include <string>
#include <string_view>

TEST(EvictionPolicyTesting, ConfigAcceptsOnlySupportedPolicies) {
    for (const std::string_view policy : {"lru", "noeviction"}) {
        const std::array<std::string_view, 4> arguments{"CONFIG", "SET", "eviction_policy", policy};
        const auto validation = KV::ValidateCommand(std::span<const std::string_view>(arguments));
        ASSERT_TRUE(validation) << validation.error;
        const auto& config = std::get<KV::ConfigParams>(validation.command->parameters);
        EXPECT_EQ(config.parameter, "eviction_policy");
        ASSERT_EQ(config.values.size(), 1U);
        EXPECT_EQ(config.values.front(), policy);
    }

    const std::array<std::string_view, 4> invalid{"CONFIG", "SET", "eviction_policy", "random"};
    const auto validation = KV::ValidateCommand(std::span<const std::string_view>(invalid));
    ASSERT_FALSE(validation);
    EXPECT_EQ(validation.error, "ERR CONFIG SET failed - 'eviction_policy' wants 'lru' or 'noeviction'");
}

TEST(EvictionPolicyTesting, NoevictionPreservesKeysAndRejectsNewKeysAtCapacity) {
    KV::NoEvictionStore<std::string, std::string> store(2);
    EXPECT_EQ(store.eviction_policy(), KV::EvictionPolicy::kNoEviction);
    store.set("old", "first");
    store.set("new", "second");

    EXPECT_EQ(store.get("old"), "first");
    EXPECT_FALSE(store.set("extra", "third"));
    EXPECT_TRUE(store.set("old", "updated"));
    EXPECT_EQ(store.size(), 2U);
    EXPECT_EQ(store.get("old"), "updated");
    EXPECT_EQ(store.get("new"), "second");

    store.set_eviction_policy(KV::EvictionPolicy::kLRU);
    store.set("extra", "third");
    EXPECT_EQ(store.size(), 2U);
    EXPECT_FALSE(store.get("old"));
    EXPECT_EQ(store.get("extra"), "third");
}
