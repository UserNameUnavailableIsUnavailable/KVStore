#include <gtest/gtest.h>

#include <Application/server/store.hpp>
#include <chrono>
#include <string>

namespace {
template <typename StoreType>
class StoreTesting : public testing::Test {};

using Stores = testing::Types<KV::LRUStore<std::string, std::string>, KV::LFUStore<std::string, std::string>>;
TYPED_TEST_SUITE(StoreTesting, Stores);

TYPED_TEST(StoreTesting, SetOwnsValuesAndOverwritesWithoutDeleting) {
    TypeParam store;
    KV::Store<std::string, std::string>& api = store;
    std::string value(4096, 'a');
    api.set("key", value);
    value[0] = 'b';
    EXPECT_EQ(api.get("key"), std::string(4096, 'a'));

    const char* original_buffer = nullptr;
    store.visit_live([&](const auto&, const std::string& stored, const auto&) { original_buffer = stored.data(); });
    api.set("key", value);
    store.visit_live([&](const auto&, const std::string& stored, const auto&) {
        EXPECT_EQ(stored, value);
        EXPECT_EQ(stored.data(), original_buffer);
    });
    EXPECT_EQ(store.size(), 1U);
    EXPECT_EQ(value.size(), 4096U);

    api.set("key", "");
    EXPECT_EQ(api.get("key"), "");
    EXPECT_EQ(store.size(), 1U);
    api.set("", std::string("a\0b", 3));
    EXPECT_EQ(api.get(""), std::string("a\0b", 3));
}

TYPED_TEST(StoreTesting, EraseReportsLiveKeysAndAllowsReinsertion) {
    TypeParam store(1);
    KV::Store<std::string, std::string>& api = store;
    EXPECT_FALSE(api.erase("missing"));
    api.set("key", "value");
    EXPECT_FALSE(api.erase("missing"));
    EXPECT_EQ(store.size(), 1U);
    EXPECT_TRUE(api.erase("key"));
    EXPECT_FALSE(api.erase("key"));
    EXPECT_EQ(store.size(), 0U);
    EXPECT_FALSE(api.get("key"));
    api.set("key", "replacement");
    EXPECT_EQ(api.get("key"), "replacement");
    EXPECT_EQ(store.size(), 1U);

    ASSERT_TRUE(api.set_ttl("key", std::chrono::steady_clock::duration::zero()));
    EXPECT_FALSE(api.erase("key"));
    EXPECT_EQ(store.size(), 0U);
    api.set("key", "fresh");
    EXPECT_EQ(api.get("key"), "fresh");
}

TYPED_TEST(StoreTesting, SetPreservesTTLAndPromotesOverwrite) {
    TypeParam store(2);
    store.set("a", "first");
    store.set("b", "second");
    ASSERT_TRUE(store.set_ttl("a", std::chrono::hours(1)));
    store.set("a", "updated");
    store.visit_live([](const std::string& key, const auto&, const auto& ttl) {
        if (key == "a") {
            EXPECT_TRUE(ttl.has_value());
        }
    });
    store.set("c", "third");
    EXPECT_EQ(store.get("a"), "updated");
    EXPECT_FALSE(store.get("b"));
    EXPECT_EQ(store.get("c"), "third");
}

TEST(StorePolicyTesting, LFUEraseRemovesFrequencyBookkeeping) {
    KV::LFUStore<std::string, std::string> store(2);
    store.set("a", "first");
    store.set("b", "second");
    store.get("a");
    ASSERT_EQ(store.frequency("a"), 2U);
    ASSERT_TRUE(store.erase("a"));
    EXPECT_EQ(store.frequency("a"), 0U);
    store.set("a", "replacement");
    EXPECT_EQ(store.frequency("a"), 1U);
    store.get("b");
    store.set("c", "third");
    EXPECT_EQ(store.frequency("a"), 0U);
    EXPECT_EQ(store.get("b"), "second");
    EXPECT_EQ(store.get("c"), "third");
}

struct CountedValue {
    static inline int copies = 0;
    static inline int assignments = 0;

    CountedValue() = default;
    CountedValue(const CountedValue&) { ++copies; }
    CountedValue(CountedValue&&) = default;
    CountedValue& operator=(const CountedValue&) {
        ++assignments;
        return *this;
    }
    CountedValue& operator=(CountedValue&&) = default;
};

template <typename StoreType>
void CheckValueCopies() {
    StoreType store;
    const CountedValue value;
    CountedValue::copies = CountedValue::assignments = 0;
    store.set("key", value);
    EXPECT_EQ(CountedValue::copies, 1);
    EXPECT_EQ(CountedValue::assignments, 0);
    CountedValue::copies = CountedValue::assignments = 0;
    store.set("key", value);
    EXPECT_EQ(CountedValue::copies, 0);
    EXPECT_EQ(CountedValue::assignments, 1);
}

TEST(StorePolicyTesting, SetDoesNotConstructIntermediateValueCopies) {
    CheckValueCopies<KV::LRUStore<std::string, CountedValue>>();
    CheckValueCopies<KV::LFUStore<std::string, CountedValue>>();
}
}  // namespace
