#include <gtest/gtest.h>

#include <chrono>
#include <memory_resource>
#include <optional>
#include <string>
#include <thread>

#include "DataStore.hpp"

namespace
{
using LruStore = KV::LRUDataStore<std::string, std::string, KV::HashMap>;
using LruTreeStore = KV::LRUDataStore<std::string, std::string, KV::RedBlackTreeMap>;
using LruArrayStore = KV::LRUDataStore<std::string, std::string, KV::ArrayMap>;

using LfuStore = KV::LFUDataStore<std::string, std::string, KV::HashMap>;
using LfuTreeStore = KV::LFUDataStore<std::string, std::string, KV::RedBlackTreeMap>;

template <typename Store>
class TtlStore final : public Store
{
public:
    using Store::Store;

    template <typename Rep, typename Period>
    bool set_ttl(const std::string& key, std::chrono::duration<Rep, Period> ttl)
    {
        const auto record = this->find(key);
        if (record == this->NoRecord())
        {
            return false;
        }
        record->set_ttl(std::optional {ttl});
        return true;
    }
};

using TtlLruStore = TtlStore<LruStore>;
using TtlLfuStore = TtlStore<LfuStore>;
} // namespace

// =============================================================================
// CRUD: identical for every strategy, over every indexing container
// =============================================================================

template <typename Store>
void ExpectBasicCrud()
{
    Store store;

    EXPECT_FALSE(store.Get("missing").has_value());
    EXPECT_FALSE(store.Exists("missing"));

    store.Set("key", "value");
    ASSERT_TRUE(store.Get("key").has_value());
    EXPECT_EQ(*store.Get("key"), "value");
    EXPECT_TRUE(store.Exists("key"));
    EXPECT_EQ(store.Size(), 1u);

    store.Set("key", "updated"); // update in place
    EXPECT_EQ(*store.Get("key"), "updated");
    EXPECT_EQ(store.Size(), 1u);

    store.Set("key", std::nullopt); // delete
    EXPECT_FALSE(store.Get("key").has_value());
    EXPECT_FALSE(store.Exists("key"));
    EXPECT_EQ(store.Size(), 0u);

    store.Set("gone", std::nullopt); // deleting a missing key is a no-op
    EXPECT_EQ(store.Size(), 0u);
}

TEST(DataStoreTesting, LruCrudWithHashIndex)
{
    ExpectBasicCrud<LruStore>();
}

TEST(DataStoreTesting, LruCrudWithTreeIndex)
{
    ExpectBasicCrud<LruTreeStore>();
}

TEST(DataStoreTesting, LruCrudWithArrayIndex)
{
    ExpectBasicCrud<LruArrayStore>();
}

TEST(DataStoreTesting, LfuCrudWithHashIndex)
{
    ExpectBasicCrud<LfuStore>();
}

TEST(DataStoreTesting, LfuCrudWithTreeIndex)
{
    ExpectBasicCrud<LfuTreeStore>();
}

template <typename Store>
void ExpectManyKeysHeldIndependently()
{
    Store store;
    for (int i = 0; i < 64; ++i)
    {
        store.Set("key" + std::to_string(i), "value" + std::to_string(i));
    }

    EXPECT_EQ(store.Size(), 64u);
    for (int i = 0; i < 64; ++i)
    {
        const auto value = store.Get("key" + std::to_string(i));
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, "value" + std::to_string(i));
    }
}

TEST(DataStoreTesting, LruHoldsManyKeysIndependently)
{
    ExpectManyKeysHeldIndependently<LruStore>();
}

TEST(DataStoreTesting, LfuHoldsManyKeysIndependently)
{
    ExpectManyKeysHeldIndependently<LfuStore>();
}

TEST(DataStoreTesting, UsesInjectedMemoryResource)
{
    std::pmr::monotonic_buffer_resource resource(8192);
    LfuStore store(0, &resource);

    store.Set("key", "value");
    EXPECT_EQ(*store.Get("key"), "value");
}

TEST(DataStoreTesting, UnboundedByDefault)
{
    LruStore store; // capacity == 0
    for (int i = 0; i < 100; ++i)
    {
        store.Set("key" + std::to_string(i), "value");
    }
    EXPECT_EQ(store.Size(), 100u);
    EXPECT_FALSE(store.IsFull());
}

TEST(DataStoreTesting, ReportsCapacityAndFullness)
{
    LruStore store(2);
    EXPECT_EQ(store.Capacity(), 2u);
    EXPECT_FALSE(store.IsFull());
    store.Set("a", "1");
    EXPECT_FALSE(store.IsFull());
    store.Set("b", "2");
    EXPECT_TRUE(store.IsFull());
}

// =============================================================================
// Node recycling (shared machinery)
// =============================================================================

TEST(DataStoreTesting, removedNodesAreRecycled)
{
    LruStore store(1);
    for (int i = 0; i < 32; ++i)
    {
        store.Set("key" + std::to_string(i), "value" + std::to_string(i));
        EXPECT_EQ(store.Size(), 1u);
    }

    // Only the newest key survives, and it reused a recycled node.
    EXPECT_TRUE(store.Exists("key31"));
    EXPECT_EQ(*store.Get("key31"), "value31");
    EXPECT_FALSE(store.Exists("key30"));
}

TEST(DataStoreTesting, RecycledNodeResetsTheLifecycle)
{
    LruStore store(1);
    store.Set("a", "1");
    store.Set("a", std::nullopt); // parked on the free list
    store.Set("b", "2"); // reuses the node

    ASSERT_TRUE(store.Exists("b"));
    EXPECT_EQ(*store.Get("b"), "2");
    EXPECT_FALSE(store.Exists("a"));
}

// =============================================================================
// LRU: the record order is the whole policy
// =============================================================================

TEST(DataStoreTesting, LruEvictsTheLeastRecentlyUsed)
{
    LruStore store(2);
    store.Set("a", "1");
    store.Set("b", "2");
    EXPECT_EQ(*store.Get("a"), "1"); // "a" becomes the most recently used
    store.Set("c", "3"); // so "b" is the victim

    EXPECT_EQ(store.Size(), 2u);
    EXPECT_TRUE(store.Exists("a"));
    EXPECT_FALSE(store.Exists("b"));
    EXPECT_TRUE(store.Exists("c"));
}

TEST(DataStoreTesting, LruCountsWritesAsUse)
{
    LruStore store(2);
    store.Set("a", "1");
    store.Set("b", "2");
    store.Set("a", "1-updated"); // a write is a use too
    store.Set("c", "3"); // so "b" is the victim

    EXPECT_TRUE(store.Exists("a"));
    EXPECT_FALSE(store.Exists("b"));
    EXPECT_TRUE(store.Exists("c"));
    EXPECT_EQ(*store.Get("a"), "1-updated");
}

TEST(DataStoreTesting, LruCountsExistsAsUse)
{
    LruStore store(2);
    store.Set("a", "1");
    store.Set("b", "2");
    EXPECT_TRUE(store.Exists("a"));
    store.Set("c", "3");

    EXPECT_TRUE(store.Exists("a"));
    EXPECT_FALSE(store.Exists("b"));
}

TEST(DataStoreTesting, LruEvictsInUseOrderNotinsertionOrder)
{
    LruStore store(3);
    store.Set("a", "1");
    store.Set("b", "2");
    store.Set("c", "3");

    store.Get("a"); // use order is now c, b, a  ->  oldest is "b"... then:
    store.Get("b"); // use order is now a, c, b  ->  oldest use is "c"
    store.Set("d", "4");

    EXPECT_EQ(store.Size(), 3u);
    EXPECT_TRUE(store.Exists("a"));
    EXPECT_TRUE(store.Exists("b"));
    EXPECT_FALSE(store.Exists("c"));
    EXPECT_TRUE(store.Exists("d"));
}

TEST(DataStoreTesting, LruSurvivesRepeatedRotation)
{
    LruStore store(3);
    store.Set("a", "1");
    store.Set("b", "2");
    store.Set("c", "3");

    for (int round = 0; round < 10; ++round)
    {
        EXPECT_TRUE(store.Get("a").has_value());
        EXPECT_TRUE(store.Get("b").has_value());
        EXPECT_TRUE(store.Get("c").has_value());
    }

    store.Set("d", "4"); // "a" is the least recently used of the three
    EXPECT_EQ(store.Size(), 3u);
    EXPECT_FALSE(store.Exists("a"));
    EXPECT_TRUE(store.Exists("d"));
}

// =============================================================================
// LFU: frequency buckets, the store's own private state
// =============================================================================

TEST(DataStoreTesting, LfuKeepsTheMostFrequentlyUsed)
{
    LfuStore store(3);
    store.Set("hot", "1");
    store.Set("warm", "2");
    store.Set("cold", "3");

    for (int i = 0; i < 5; ++i)
    {
        store.Get("hot");
    }
    store.Get("warm");

    store.Set("new", "4"); // "cold" has the lowest use count

    EXPECT_EQ(store.Size(), 3u);
    EXPECT_TRUE(store.Exists("hot"));
    EXPECT_TRUE(store.Exists("warm"));
    EXPECT_FALSE(store.Exists("cold"));
    EXPECT_TRUE(store.Exists("new"));
}

TEST(DataStoreTesting, LfuTracksUseCountPerKey)
{
    LfuStore store;
    store.Set("a", "1");
    EXPECT_EQ(store.FrequencyOf("a"), 1u); // born with one use

    store.Get("a");
    store.Get("a");
    EXPECT_EQ(store.FrequencyOf("a"), 3u);

    store.Exists("a"); // a lookup is a use
    EXPECT_EQ(store.FrequencyOf("a"), 4u);

    store.Set("a", "updated"); // a write is a use
    EXPECT_EQ(store.FrequencyOf("a"), 5u);

    EXPECT_EQ(store.FrequencyOf("never-stored"), 0u);
}

TEST(DataStoreTesting, LfuForgetsTheCountWhenARecordLeaves)
{
    LfuStore store;
    store.Set("a", "1");
    store.Get("a");
    ASSERT_EQ(store.FrequencyOf("a"), 2u);

    store.Set("a", std::nullopt); // removal must clear the bookkeeping
    EXPECT_EQ(store.FrequencyOf("a"), 0u);

    store.Set("a", "reborn"); // so the reborn record starts from scratch
    EXPECT_EQ(store.FrequencyOf("a"), 1u);
}

TEST(DataStoreTesting, LfuBreaksTiesByLeastRecentlyUsed)
{
    LfuStore store(3);
    store.Set("a", "1"); // all three end up with a use count of one
    store.Set("b", "2");
    store.Set("c", "3");

    store.Set("d", "4"); // "a" is the least recent among equals

    EXPECT_FALSE(store.Exists("a"));
    EXPECT_TRUE(store.Exists("b"));
    EXPECT_TRUE(store.Exists("c"));
    EXPECT_TRUE(store.Exists("d"));
}

TEST(DataStoreTesting, LfuIgnoresRecencyWhenCountsDiffer)
{
    LfuStore store(2);
    store.Set("old-but-hot", "1");
    store.Get("old-but-hot");
    store.Get("old-but-hot"); // count 3
    store.Set("new-but-cold", "2"); // count 1

    store.Set("newest", "3"); // recency would drop "old-but-hot"; frequency does not

    EXPECT_TRUE(store.Exists("old-but-hot"));
    EXPECT_FALSE(store.Exists("new-but-cold"));
    EXPECT_TRUE(store.Exists("newest"));
}

TEST(DataStoreTesting, LfuEmptiedBucketsDoNotStrandTheVictimSearch)
{
    LfuStore store(2);
    store.Set("a", "1");
    store.Set("b", "2");

    // Push both out of the count-1 bucket, leaving it empty.
    store.Get("a");
    store.Get("b");
    EXPECT_EQ(store.FrequencyOf("a"), 2u);
    EXPECT_EQ(store.FrequencyOf("b"), 2u);

    store.Get("b"); // "b" moves to count 3, so "a" is now the least used
    store.Set("c", "3");

    EXPECT_FALSE(store.Exists("a"));
    EXPECT_TRUE(store.Exists("b"));
    EXPECT_TRUE(store.Exists("c"));
}

TEST(DataStoreTesting, LfuStaysConsistentUnderChurn)
{
    LfuStore store(4);
    store.Set("keep", "0");

    for (int i = 0; i < 50; ++i)
    {
        store.Get("keep"); // keep its count far above everyone else's
        store.Set("churn" + std::to_string(i), "value");
        EXPECT_LE(store.Size(), 4u);
        EXPECT_TRUE(store.Exists("keep"));
    }

    EXPECT_EQ(*store.Get("keep"), "0");
}

TEST(DataStoreTesting, LfuRecycledNodesKeepBucketsExact)
{
    LfuStore store(1);
    for (int i = 0; i < 16; ++i)
    {
        const std::string key = "key" + std::to_string(i);
        store.Set(key, "value" + std::to_string(i));
        EXPECT_EQ(store.Size(), 1u);
        EXPECT_EQ(store.FrequencyOf(key), 1u);
        if (i > 0)
        {
            EXPECT_EQ(store.FrequencyOf("key" + std::to_string(i - 1)), 0u);
        }
    }
}

TEST(DataStoreTesting, ExpiredRecordsAreDroppedOnAccess)
{
    TtlLruStore store;
    store.Set("key", "value");
    ASSERT_TRUE(store.set_ttl("key", std::chrono::milliseconds(10)));
    EXPECT_TRUE(store.Exists("key"));
    EXPECT_EQ(store.Size(), 1u);

    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    EXPECT_FALSE(store.Get("key").has_value()); // lazy expiry
    EXPECT_EQ(store.Size(), 0u); // and the record is gone
    EXPECT_FALSE(store.Exists("key"));
}

TEST(DataStoreTesting, WritingRevivesAnExpiredRecord)
{
    TtlLruStore store;
    store.Set("key", "value");
    ASSERT_TRUE(store.set_ttl("key", std::chrono::milliseconds(10)));
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    store.Set("key", "rewritten"); // the expired record was dropped, this is new
    EXPECT_EQ(store.Size(), 1u);
    ASSERT_TRUE(store.Get("key").has_value());
    EXPECT_EQ(*store.Get("key"), "rewritten");
}

TEST(DataStoreTesting, ExpiryKeepsLfuBucketsExact)
{
    TtlLfuStore store;
    store.Set("key", "value");
    ASSERT_TRUE(store.set_ttl("key", std::chrono::milliseconds(10)));
    store.Get("key");
    ASSERT_EQ(store.FrequencyOf("key"), 2u);

    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    EXPECT_FALSE(store.Exists("key")); // expiry routed through Drop
    EXPECT_EQ(store.FrequencyOf("key"), 0u); // so the bookkeeping went with it
    EXPECT_EQ(store.Size(), 0u);
}

TEST(DataStoreTesting, RecycledNodeGetsAFreshLifecycle)
{
    TtlLruStore store(1);
    store.Set("a", "1");
    ASSERT_TRUE(store.set_ttl("a", std::chrono::milliseconds(10)));
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    // The expired node is recycled to make room for "b".
    store.Set("b", "2");
    EXPECT_EQ(store.Size(), 1u);

    // "b" must not inherit "a"'s expired timestamp.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(store.Exists("b"));
    EXPECT_EQ(*store.Get("b"), "2");
}
