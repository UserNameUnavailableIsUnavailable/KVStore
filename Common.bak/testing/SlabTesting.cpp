#include "Slab.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace
{
// Counts live instances so tests can prove the slab destroys what it owns.
struct Tracked
{
    inline static int live = 0;
    inline static int constructed = 0;

    explicit Tracked(int initial) :
        value(initial)
    {
        ++live;
        ++constructed;
    }

    Tracked(const Tracked&) = delete;
    Tracked& operator=(const Tracked&) = delete;

    ~Tracked() { --live; }

    static void Reset() noexcept
    {
        live = 0;
        constructed = 0;
    }

    int value;
};

// Neither default-constructible nor copyable: the slab must build it in place.
struct InPlaceOnly
{
    InPlaceOnly(int first, std::string second) :
        first(first),
        second(std::move(second))
    {
    }

    InPlaceOnly(const InPlaceOnly&) = delete;
    InPlaceOnly& operator=(const InPlaceOnly&) = delete;

    int first;
    std::string second;
};

struct Exploding
{
    explicit Exploding(bool should_throw)
    {
        if (should_throw)
        {
            throw std::runtime_error("construction failed");
        }
    }
};

using IntSlab = KV::Slab<int, 4>; // tiny blocks, so growth is easy to observe
using TrackedSlab = KV::Slab<Tracked, 4>;
} // namespace

// =============================================================================
// Handles
// =============================================================================

TEST(SlabTesting, DefaultHandleRefersToNothing)
{
    IntSlab slab;
    const IntSlab::Handle handle;

    EXPECT_FALSE(handle.IsValid());
    EXPECT_FALSE(slab.Contains(handle));
    EXPECT_EQ(slab.find(handle), nullptr);
}

TEST(SlabTesting, AcquireYieldsAValidHandleAndUsableReference)
{
    IntSlab slab;
    auto [handle, value] = slab.Acquire(42);

    EXPECT_TRUE(handle.IsValid());
    EXPECT_EQ(value, 42);
    EXPECT_EQ(slab.find(handle), &value); // the reference and the lookup agree
    EXPECT_TRUE(slab.Contains(handle));
    EXPECT_EQ(slab.Size(), 1u);
}

TEST(SlabTesting, HandleSurvivesTheKernelRoundTrip)
{
    IntSlab slab;
    const auto [handle, value] = slab.Acquire(7);

    // What io_uring's user_data and epoll's data.u64 do to a handle.
    const IntSlab::Handle decoded = IntSlab::Handle::Decode(handle.Encode());

    EXPECT_EQ(decoded, handle);
    EXPECT_EQ(slab.find(decoded), &value);
}

TEST(SlabTesting, EncodedInvalidHandleStaysInvalid)
{
    const IntSlab::Handle handle;
    const IntSlab::Handle decoded = IntSlab::Handle::Decode(handle.Encode());

    EXPECT_FALSE(decoded.IsValid());
}

TEST(SlabTesting, HandlesFromDifferentSlotsDiffer)
{
    IntSlab slab;
    const auto [first, first_value] = slab.Acquire(1);
    const auto [second, second_value] = slab.Acquire(2);

    EXPECT_NE(first, second);
    EXPECT_NE(&first_value, &second_value);
    EXPECT_EQ(slab.Size(), 2u);
}

// =============================================================================
// Generations: the point of the whole design
// =============================================================================

TEST(SlabTesting, ReleasedHandleNoLongerresolves)
{
    IntSlab slab;
    const auto [handle, value] = slab.Acquire(42);

    EXPECT_TRUE(slab.Release(handle));

    EXPECT_FALSE(slab.Contains(handle));
    EXPECT_EQ(slab.find(handle), nullptr);
    EXPECT_EQ(slab.Size(), 0u);
}

TEST(SlabTesting, StaleHandleDoesNotresolveToTheSlotsNewOccupant)
{
    IntSlab slab;
    const auto [stale,ignored] = slab.Acquire(1);
    ASSERT_TRUE(slab.Release(stale));

    // The freed slot is handed straight back out, so the index collides.
    const auto [fresh, fresh_value] = slab.Acquire(2);
    ASSERT_EQ(fresh.index, stale.index);
    ASSERT_NE(fresh.generation, stale.generation);

    // This is the late-completion case: the old handle must not find the new
    // object sitting in its slot.
    EXPECT_EQ(slab.find(stale), nullptr);
    EXPECT_FALSE(slab.Contains(stale));
    EXPECT_EQ(slab.find(fresh), &fresh_value);
    EXPECT_EQ(fresh_value, 2);
}

TEST(SlabTesting, GenerationAdvancesOnEveryRelease)
{
    IntSlab slab;
    std::unordered_set<std::uint64_t> seen;

    for (int round = 0; round < 8; ++round)
    {
        const auto [handle, value] = slab.Acquire(round);
        EXPECT_TRUE(seen.insert(handle.Encode()).second) << "handle reused at round " << round;
        ASSERT_TRUE(slab.Release(handle));
    }
}

TEST(SlabTesting, ReleasingTwiceIsRejectedRatherThanFatal)
{
    IntSlab slab;
    const auto [handle, value] = slab.Acquire(42);

    EXPECT_TRUE(slab.Release(handle));
    EXPECT_FALSE(slab.Release(handle)); // a late event, not an error
    EXPECT_EQ(slab.Size(), 0u);
}

TEST(SlabTesting, OutOfRangeHandleIsRejected)
{
    IntSlab slab;
    slab.Acquire(1);

    const IntSlab::Handle beyond {.index = 10'000, .generation = 1};
    EXPECT_EQ(slab.find(beyond), nullptr);
    EXPECT_FALSE(slab.Release(beyond));
}

TEST(SlabTesting, HandleIntoAFreeSlotIsRejected)
{
    IntSlab slab;
    const auto [first, first_value] = slab.Acquire(1);
    const auto [second, second_value] = slab.Acquire(2);
    ASSERT_TRUE(slab.Release(second));

    // Same generation, but the slot is free: still must not resolve.
    const IntSlab::Handle into_free_slot {.index = second.index, .generation = second.generation};
    EXPECT_EQ(slab.find(into_free_slot), nullptr);
    EXPECT_EQ(slab.find(first), &first_value);
}

// =============================================================================
// Address stability
// =============================================================================

TEST(SlabTesting, ObjectsNeverMoveAsTheSlabGrows)
{
    IntSlab slab; // blocks of 4, so this crosses many block boundaries
    std::vector<IntSlab::Handle> handles;
    std::vector<int*> addresses;

    for (int i = 0; i < 200; ++i)
    {
        auto [handle, value] = slab.Acquire(i);
        handles.push_back(handle);
        addresses.push_back(&value);
    }

    ASSERT_GE(slab.Capacity(), 200u);
    for (std::size_t i = 0; i < handles.size(); ++i)
    {
        // Every address handed out long ago is still the object's address, and
        // the object still holds its value.
        EXPECT_EQ(slab.find(handles[i]), addresses[i]);
        EXPECT_EQ(*addresses[i], static_cast<int>(i));
    }
}

TEST(SlabTesting, ReleasedSlotsAreReusedBeforeGrowing)
{
    IntSlab slab; // 4 slots per block
    std::vector<IntSlab::Handle> handles;
    for (int i = 0; i < 4; ++i)
    {
        handles.push_back(slab.Acquire(i).first);
    }
    const std::size_t capacity_when_full = slab.Capacity();

    ASSERT_TRUE(slab.Release(handles[1]));
    ASSERT_TRUE(slab.Release(handles[2]));
    slab.Acquire(100);
    slab.Acquire(200);

    EXPECT_EQ(slab.Capacity(), capacity_when_full); // no new block needed
    EXPECT_EQ(slab.Size(), 4u);
}

TEST(SlabTesting, CapacityGrowsOneBlockAtATime)
{
    IntSlab slab; // 4 slots per block
    EXPECT_EQ(slab.Capacity(), 0u);

    slab.Acquire(1);
    EXPECT_EQ(slab.Capacity(), 4u);

    slab.Acquire(2);
    slab.Acquire(3);
    slab.Acquire(4);
    EXPECT_EQ(slab.Capacity(), 4u); // still the first block

    slab.Acquire(5);
    EXPECT_EQ(slab.Capacity(), 8u);
}

// =============================================================================
// Object lifetime
// =============================================================================

TEST(SlabTesting, ReleaserunsTheDestructor)
{
    Tracked::Reset();
    TrackedSlab slab;

    const auto [handle, value] = slab.Acquire(1);
    EXPECT_EQ(Tracked::live, 1);

    ASSERT_TRUE(slab.Release(handle));
    EXPECT_EQ(Tracked::live, 0);
    EXPECT_EQ(Tracked::constructed, 1);
}

TEST(SlabTesting, DestructorDestroysWhatIsStillLive)
{
    Tracked::Reset();
    {
        TrackedSlab slab;
        for (int i = 0; i < 10; ++i)
        {
            slab.Acquire(i);
        }
        ASSERT_EQ(Tracked::live, 10);
    }
    EXPECT_EQ(Tracked::live, 0); // no leak on slab teardown
}

TEST(SlabTesting,ReusingASlotDoesNotDoubleDestroy)
{
    Tracked::Reset();
    TrackedSlab slab;

    for (int i = 0; i < 20; ++i)
    {
        const auto [handle, value] = slab.Acquire(i);
        EXPECT_EQ(Tracked::live, 1);
        ASSERT_TRUE(slab.Release(handle));
        EXPECT_EQ(Tracked::live, 0);
    }
    EXPECT_EQ(Tracked::constructed, 20);
}

TEST(SlabTesting, ConstructsObjectsInPlaceFromArguments)
{
    KV::Slab<InPlaceOnly, 4> slab;
    auto [handle, object] = slab.Acquire(7, std::string("payload"));

    EXPECT_EQ(object.first, 7);
    EXPECT_EQ(object.second, "payload");
    EXPECT_EQ(&object, slab.find(handle));
}

TEST(SlabTesting, HoldsMoveOnlyObjects)
{
    KV::Slab<std::unique_ptr<int>, 4> slab;
    auto [handle, pointer] = slab.Acquire(std::make_unique<int>(5));

    ASSERT_NE(slab.find(handle), nullptr);
    EXPECT_EQ(**slab.find(handle), 5);
    EXPECT_EQ(*pointer, 5);
}

TEST(SlabTesting, AFailedConstructionLeavesTheSlabUnchanged)
{
    KV::Slab<Exploding, 4> slab;
    const auto [first, first_object] = slab.Acquire(false);
    const std::size_t size_before = slab.Size();

    EXPECT_THROW(slab.Acquire(true), std::runtime_error);
    EXPECT_EQ(slab.Size(), size_before);

    // The slot the failed construction was about to use is still free.
    const auto [second, second_object] = slab.Acquire(false);
    EXPECT_TRUE(second.IsValid());
    EXPECT_TRUE(slab.Contains(first));
    EXPECT_TRUE(slab.Contains(second));
}

// =============================================================================
// Bulk access and observers
// =============================================================================

TEST(SlabTesting, ForEachVisitsOnlyLiveObjects)
{
    IntSlab slab;
    std::vector<IntSlab::Handle> handles;
    for (int i = 0; i < 10; ++i)
    {
        handles.push_back(slab.Acquire(i).first);
    }
    ASSERT_TRUE(slab.Release(handles[3]));
    ASSERT_TRUE(slab.Release(handles[7]));

    std::vector<int> visited;
    slab.ForEach([&visited](IntSlab::Handle, int& value) { visited.push_back(value); });

    ASSERT_EQ(visited.size(), 8u);
    std::ranges::sort(visited);
    EXPECT_EQ(visited, (std::vector<int> {0, 1, 2, 4, 5, 6, 8, 9}));
}

TEST(SlabTesting, ForEachHandlesresolveToTheVisitedObject)
{
    IntSlab slab;
    slab.Acquire(1);
    slab.Acquire(2);

    slab.ForEach([&slab](IntSlab::Handle handle, int& value) {
        EXPECT_EQ(slab.find(handle), &value);
    });
}

TEST(SlabTesting, ForEachMayReleaseTheVisitedObject)
{
    IntSlab slab;
    for (int i = 0; i < 10; ++i)
    {
        slab.Acquire(i);
    }

    slab.ForEach([&slab](IntSlab::Handle handle, int&) { slab.Release(handle); });

    EXPECT_TRUE(slab.Empty());
    EXPECT_EQ(slab.Size(), 0u);
}

TEST(SlabTesting, TracksSizeAcrossChurn)
{
    IntSlab slab;
    EXPECT_TRUE(slab.Empty());

    std::vector<IntSlab::Handle> handles;
    for (int i = 0; i < 32; ++i)
    {
        handles.push_back(slab.Acquire(i).first);
    }
    EXPECT_EQ(slab.Size(), 32u);
    EXPECT_FALSE(slab.Empty());

    for (const IntSlab::Handle handle : handles)
    {
        EXPECT_TRUE(slab.Release(handle));
    }
    EXPECT_TRUE(slab.Empty());
}

TEST(SlabTesting, EmptySlabAllocatesNothing)
{
    IntSlab slab;
    EXPECT_EQ(slab.Capacity(), 0u);
    EXPECT_EQ(slab.Size(), 0u);
    EXPECT_TRUE(slab.Empty());
}

// =============================================================================
// Move
// =============================================================================

TEST(SlabTesting, MoveKeepsHandlesAndAddressesValid)
{
    Tracked::Reset();
    TrackedSlab source;
    const auto [handle, value] = source.Acquire(99);
    const Tracked* address = &value;

    TrackedSlab moved(std::move(source));

    EXPECT_EQ(moved.Size(), 1u);
    EXPECT_EQ(moved.find(handle), address); // the object did not move
    EXPECT_EQ(moved.find(handle)->value, 99);
    EXPECT_EQ(Tracked::live, 1);
    EXPECT_TRUE(source.Empty());
}

TEST(SlabTesting, MoveAssignmentDestroysTheOverwrittenObjects)
{
    Tracked::Reset();
    TrackedSlab target;
    target.Acquire(1);
    target.Acquire(2);
    ASSERT_EQ(Tracked::live, 2);

    TrackedSlab source;
    const auto [handle, value] = source.Acquire(3);

    target = std::move(source);

    EXPECT_EQ(Tracked::live, 1); // the two originals are gone
    EXPECT_EQ(target.Size(), 1u);
    ASSERT_NE(target.find(handle), nullptr);
    EXPECT_EQ(target.find(handle)->value, 3);
}

// =============================================================================
// Distinct slabs
// =============================================================================

TEST(SlabTesting, IdenticalHandlesFromDifferentSlabsAreIndependent)
{
    IntSlab first;
    IntSlab second;

    const auto [first_handle, first_value] = first.Acquire(1);
    const auto [second_handle, second_value] = second.Acquire(2);

    // Same index and generation, but each only resolves in its own slab.
    ASSERT_EQ(first_handle, second_handle);
    EXPECT_EQ(*first.find(first_handle), 1);
    EXPECT_EQ(*second.find(second_handle), 2);
}
