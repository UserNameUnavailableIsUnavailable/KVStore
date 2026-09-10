#include "SessionManager.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace
{
// Stands in for a Session: counts live instances so tests can prove the manager
// destroys sessions exactly once, and at the right moment.
struct FakeSession
{
    inline static int live = 0;

    explicit FakeSession(std::string name) :
        name(std::move(name))
    {
        ++live;
    }

    FakeSession(const FakeSession&) = delete;
    FakeSession& operator=(const FakeSession&) = delete;

    ~FakeSession() { --live; }

    static void Reset() noexcept { live = 0; }

    std::string name;
    int bytes_served = 0;
};

// Stands in for a SessionTask. Destroying one models destroying a coroutine
// frame, which is the thing that must not happen while an operation is still
// outstanding - so tests watch this count closely.
struct FakeTask
{
    inline static int live = 0;
    inline static std::vector<std::string> destroyed;

    explicit FakeTask(std::string owner) :
        owner(std::move(owner))
    {
        ++live;
    }

    FakeTask(FakeTask&& other) noexcept :
        owner(std::move(other.owner)),
        moved_from(other.moved_from)
    {
        other.moved_from = true;
        ++live;
    }

    FakeTask(const FakeTask&) = delete;
    FakeTask& operator=(const FakeTask&) = delete;

    ~FakeTask()
    {
        --live;
        if (!moved_from)
        {
            destroyed.push_back(owner);
        }
    }

    static void Reset() noexcept
    {
        live = 0;
        destroyed.clear();
    }

    std::string owner;
    bool moved_from = false;
    bool done = false;
};

using Manager = KV::SessionManager<FakeSession, FakeTask, 4>; // tiny blocks
using Handle = Manager::Handle;

class SessionManagerTesting : public ::testing::Test
{
protected:
    void SetUp() override
    {
        FakeSession::Reset();
        FakeTask::Reset();
    }

    // Open a session and start it, the way a server does on accept.
    Handle Open(std::string name)
    {
        auto [handle, session] = manager.Acquire(name);
        EXPECT_EQ(session.name, name);
        EXPECT_TRUE(manager.Start(handle, FakeTask(std::move(name))));
        return handle;
    }

    Manager manager;
};
} // namespace

// =============================================================================
// Opening
// =============================================================================

TEST_F(SessionManagerTesting, AcquireConstructsTheSessionInPlace)
{
    auto [handle, session] = manager.Acquire(std::string("client-1"));

    EXPECT_EQ(session.name, "client-1");
    EXPECT_EQ(manager.find(handle), &session); // the reference and the lookup agree
    EXPECT_EQ(FakeSession::live, 1);
    EXPECT_EQ(manager.Size(), 1u);
    EXPECT_TRUE(manager.IsActive(handle));
}

TEST_F(SessionManagerTesting, ASessionStartsWithoutATask)
{
    const auto [handle, session] = manager.Acquire(std::string("client-1"));

    EXPECT_EQ(manager.GetTask(handle), nullptr);
    EXPECT_NE(manager.find(handle), nullptr); // but it is already reachable
}

TEST_F(SessionManagerTesting, StartAttachesTheTask)
{
    const auto [handle, session] = manager.Acquire(std::string("client-1"));

    ASSERT_TRUE(manager.Start(handle, FakeTask("client-1")));

    ASSERT_NE(manager.GetTask(handle), nullptr);
    EXPECT_EQ(manager.GetTask(handle)->owner, "client-1");
    EXPECT_EQ(FakeTask::live, 1); // the temporary was moved from, not duplicated
}

TEST_F(SessionManagerTesting, StartOnAnExpiredHandleDropsTheTask)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.beginClose(handle));
    ASSERT_EQ(FakeTask::live, 0);

    // The coroutine frame must not survive a session that is already gone.
    EXPECT_FALSE(manager.Start(handle, FakeTask("late")));
    EXPECT_EQ(FakeTask::live, 0);
}

TEST_F(SessionManagerTesting, SessionAddressesAreStableAsMoreOpen)
{
    std::vector<Handle> handles;
    std::vector<FakeSession*> addresses;

    for (int i = 0; i < 64; ++i) // crosses many4-slot blocks
    {
        auto [handle, session] = manager.Acquire("client-" + std::to_string(i));
        handles.push_back(handle);
        addresses.push_back(&session);
    }

    for (std::size_t i = 0; i < handles.size(); ++i)
    {
        // A coroutine that captured this reference long ago is still safe.
        EXPECT_EQ(manager.find(handles[i]), addresses[i]);
        EXPECT_EQ(addresses[i]->name, "client-" + std::to_string(i));
    }
}

// =============================================================================
// Closing with nothing outstanding
// =============================================================================

TEST_F(SessionManagerTesting, ClosingAnIdleSessionReleasesItAtOnce)
{
    const Handle handle = Open("client-1");
    ASSERT_EQ(FakeSession::live, 1);

    EXPECT_TRUE(manager.beginClose(handle)); // released before returning

    EXPECT_EQ(manager.find(handle), nullptr);
    EXPECT_FALSE(manager.Contains(handle));
    EXPECT_EQ(manager.Size(), 0u);
    EXPECT_EQ(FakeSession::live, 0);
    EXPECT_EQ(FakeTask::live, 0); // the coroutine frame went with it
    EXPECT_EQ(FakeTask::destroyed, (std::vector<std::string> {"client-1"}));
}

TEST_F(SessionManagerTesting, ClosingIsIdempotent)
{
    const Handle handle = Open("client-1");

    EXPECT_TRUE(manager.beginClose(handle));
    // Every path that notices a session is finished may call this.
    EXPECT_FALSE(manager.beginClose(handle));
    EXPECT_FALSE(manager.beginClose(handle));
    EXPECT_EQ(FakeSession::live, 0);
}

TEST_F(SessionManagerTesting, ClosingOneSessionLeavesTheOthersAlone)
{
    const Handle first = Open("client-1");
    const Handle second = Open("client-2");
    const Handle third = Open("client-3");

    ASSERT_TRUE(manager.beginClose(second));

    EXPECT_NE(manager.find(first), nullptr);
    EXPECT_EQ(manager.find(second), nullptr);
    EXPECT_NE(manager.find(third), nullptr);
    EXPECT_EQ(manager.Size(), 2u);
    EXPECT_EQ(FakeSession::live, 2);
}

// =============================================================================
// Draining: the reason this class exists
// =============================================================================

TEST_F(SessionManagerTesting, ASessionWithOutstandingWorkIsNotDestroyedOnClose)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle)); // e.g. a recv in the kernel

    EXPECT_FALSE(manager.beginClose(handle)); // not released

    EXPECT_TRUE(manager.Contains(handle)); // the slot is still held
    EXPECT_TRUE(manager.IsDraining(handle));
    EXPECT_FALSE(manager.IsActive(handle));
    // The kernel may still write into this session's buffer, so it must live on.
    EXPECT_EQ(FakeSession::live, 1);
    EXPECT_EQ(FakeTask::live, 1); // and the awaiter inside the frame must too
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 1u);
}

TEST_F(SessionManagerTesting, ADrainingSessionIsWithheldFromfind)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle));
    ASSERT_FALSE(manager.beginClose(handle));

    // Nothing new should be started on a session on its way out.
    EXPECT_EQ(manager.find(handle), nullptr);
    EXPECT_EQ(manager.GetActiveCount(), 0u);
    EXPECT_EQ(manager.GetDrainingCount(), 1u);
    EXPECT_EQ(manager.Size(), 1u);
}

TEST_F(SessionManagerTesting, ADrainingSessionRefusesNewSubmissions)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle));
    ASSERT_FALSE(manager.beginClose(handle));

    EXPECT_FALSE(manager.OnOperationSubmitted(handle));
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 1u); // unchanged
}

TEST_F(SessionManagerTesting, TheLastSettledOperationReleasesADrainingSession)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle));
    ASSERT_FALSE(manager.beginClose(handle));
    ASSERT_EQ(FakeSession::live, 1);

    // The completion the kernel still owed us finally arrives.
    EXPECT_EQ(manager.OnOperationSettled(handle), nullptr); // discard the event

    EXPECT_FALSE(manager.Contains(handle));
    EXPECT_EQ(manager.Size(), 0u);
    EXPECT_EQ(FakeSession::live, 0);
    EXPECT_EQ(FakeTask::live, 0);
}

TEST_F(SessionManagerTesting, EveryOutstandingOperationMustSettleFirst)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle)); // a read
    ASSERT_TRUE(manager.OnOperationSubmitted(handle)); // a write
    ASSERT_TRUE(manager.OnOperationSubmitted(handle)); // a timeout
    ASSERT_EQ(manager.GetPendingOperationCount(handle), 3u);

    ASSERT_FALSE(manager.beginClose(handle));

    manager.OnOperationSettled(handle);
    EXPECT_EQ(FakeSession::live, 1); // two still outstanding
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 2u);

    manager.OnOperationSettled(handle);
    EXPECT_EQ(FakeSession::live, 1); // one still outstanding
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 1u);

    manager.OnOperationSettled(handle);
    EXPECT_EQ(FakeSession::live, 0); // now it is safe
    EXPECT_FALSE(manager.Contains(handle));
}

TEST_F(SessionManagerTesting, SettlingWorkOnAnActiveSessionReturnsIt)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle));

    FakeSession* session = manager.OnOperationSettled(handle);

    ASSERT_NE(session, nullptr); // the event should be acted on
    EXPECT_EQ(session->name, "client-1");
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 0u);
    EXPECT_TRUE(manager.IsActive(handle)); // an active session is not released
    EXPECT_EQ(FakeSession::live, 1);
}

TEST_F(SessionManagerTesting, WorkMaySettleAndBeResubmittedRepeatedly)
{
    const Handle handle = Open("client-1");

    for (int round = 0; round < 100; ++round) // a long-lived connection
    {
        ASSERT_TRUE(manager.OnOperationSubmitted(handle));
        FakeSession* session = manager.OnOperationSettled(handle);
        ASSERT_NE(session, nullptr);
        session->bytes_served += 10;
    }

    ASSERT_NE(manager.find(handle), nullptr);
    EXPECT_EQ(manager.find(handle)->bytes_served, 1000);
    EXPECT_EQ(manager.Size(), 1u);
}

// =============================================================================
// Late events
// =============================================================================

TEST_F(SessionManagerTesting, AnExpiredHandleIsRejectedEverywhere)
{
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.beginClose(handle));

    EXPECT_EQ(manager.find(handle), nullptr);
    EXPECT_EQ(manager.GetTask(handle), nullptr);
    EXPECT_FALSE(manager.OnOperationSubmitted(handle));
    EXPECT_EQ(manager.OnOperationSettled(handle), nullptr);
    EXPECT_FALSE(manager.IsActive(handle));
    EXPECT_FALSE(manager.IsDraining(handle));
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 0u);
}

TEST_F(SessionManagerTesting, AStaleHandleDoesNotresolveToTheSlotsNewSession)
{
    const Handle stale = Open("old-client");
    ASSERT_TRUE(manager.beginClose(stale));

    // The freed slot is handed straight back out, so the index collides.
    const Handle fresh = Open("new-client");
    ASSERT_EQ(fresh.index, stale.index);
    ASSERT_NE(fresh.generation, stale.generation);

    // A completion left over from the old connection must not touch the new one.
    EXPECT_EQ(manager.find(stale), nullptr);
    EXPECT_EQ(manager.OnOperationSettled(stale), nullptr);

    ASSERT_NE(manager.find(fresh), nullptr);
    EXPECT_EQ(manager.find(fresh)->name, "new-client");
    EXPECT_EQ(manager.GetPendingOperationCount(fresh), 0u); // its count was untouched
}

TEST_F(SessionManagerTesting, ADefaultHandleIsInert)
{
    Open("client-1");
    const Handle unset;

    EXPECT_EQ(manager.find(unset), nullptr);
    EXPECT_FALSE(manager.OnOperationSubmitted(unset));
    EXPECT_EQ(manager.OnOperationSettled(unset), nullptr);
    EXPECT_EQ(manager.Size(), 1u); // the real session is untouched
}

TEST_F(SessionManagerTesting, SettlingMoreOftenThanSubmittingDoesNotUnderflow)
{
    const Handle handle = Open("client-1");

    // A caller reporting a completion it never submitted must not wrap the
    // count around, which would make the session immortal.
    EXPECT_NE(manager.OnOperationSettled(handle), nullptr);
    EXPECT_EQ(manager.GetPendingOperationCount(handle), 0u);

    EXPECT_TRUE(manager.beginClose(handle)); // still releases
    EXPECT_EQ(FakeSession::live, 0);
}

// =============================================================================
// Shutdown
// =============================================================================

TEST_F(SessionManagerTesting, CloseAllReleasesIdleSessionsAndDrainsTheRest)
{
    const Handle idle = Open("idle");
    const Handle busy = Open("busy");
    ASSERT_TRUE(manager.OnOperationSubmitted(busy));

    manager.CloseAll();

    EXPECT_FALSE(manager.Contains(idle));
    EXPECT_TRUE(manager.IsDraining(busy));
    EXPECT_EQ(manager.Size(), 1u);
    EXPECT_EQ(FakeSession::live, 1);

    manager.OnOperationSettled(busy); // the straggler comes back
    EXPECT_TRUE(manager.Empty());
    EXPECT_EQ(FakeSession::live, 0);
}

TEST_F(SessionManagerTesting, CloseAllHandlesManySessions)
{
    for (int i = 0; i < 50; ++i)
    {
        Open("client-" + std::to_string(i));
    }
    ASSERT_EQ(manager.Size(), 50u);

    manager.CloseAll();

    EXPECT_TRUE(manager.Empty());
    EXPECT_EQ(FakeSession::live, 0);
    EXPECT_EQ(FakeTask::live, 0);
    EXPECT_EQ(FakeTask::destroyed.size(), 50u);
}

TEST_F(SessionManagerTesting, DestroyingTheManagerDestroysWhatItHolds)
{
    {
        Manager local;
        for (int i = 0; i < 10; ++i)
        {
            auto [handle, session] = local.Acquire("client-" + std::to_string(i));
            ASSERT_TRUE(local.Start(handle, FakeTask("client-" + std::to_string(i))));
            local.OnOperationSubmitted(handle); // still outstanding at teardown
        }
        ASSERT_EQ(FakeSession::live, 10);
    }

    EXPECT_EQ(FakeSession::live, 0);
    EXPECT_EQ(FakeTask::live, 0);
}

// =============================================================================
// Iteration and counts
// =============================================================================

TEST_F(SessionManagerTesting, ForEachActiveSkipsDrainingSessions)
{
    Open("active-1");
    const Handle draining = Open("draining");
    Open("active-2");
    ASSERT_TRUE(manager.OnOperationSubmitted(draining));
    ASSERT_FALSE(manager.beginClose(draining));

    std::vector<std::string> visited;
    manager.ForEachActive([&visited](Handle, FakeSession& session) {
        visited.push_back(session.name);
    });

    std::ranges::sort(visited);
    EXPECT_EQ(visited, (std::vector<std::string> {"active-1", "active-2"}));
}

TEST_F(SessionManagerTesting, ForEachActiveHandlesresolveToTheVisitedSession)
{
    Open("client-1");
    Open("client-2");

    manager.ForEachActive([this](Handle handle, FakeSession& session) {
        EXPECT_EQ(manager.find(handle), &session);
    });
}

TEST_F(SessionManagerTesting, ForEachActiveMayCloseTheVisitedSession)
{
    for (int i = 0; i < 10; ++i)
    {
        Open("client-" + std::to_string(i));
    }

    manager.ForEachActive([this](Handle handle, FakeSession&) { manager.beginClose(handle); });

    EXPECT_TRUE(manager.Empty());
    EXPECT_EQ(FakeSession::live, 0);
}

TEST_F(SessionManagerTesting, ReportsActiveAndDrainingCounts)
{
    const Handle first = Open("client-1");
    const Handle second = Open("client-2");
    Open("client-3");

    EXPECT_EQ(manager.GetActiveCount(), 3u);
    EXPECT_EQ(manager.GetDrainingCount(), 0u);

    ASSERT_TRUE(manager.OnOperationSubmitted(first));
    ASSERT_TRUE(manager.OnOperationSubmitted(second));
    ASSERT_FALSE(manager.beginClose(first));
    ASSERT_FALSE(manager.beginClose(second));

    EXPECT_EQ(manager.GetActiveCount(), 1u);
    EXPECT_EQ(manager.GetDrainingCount(), 2u);
    EXPECT_EQ(manager.Size(), 3u);
}

TEST_F(SessionManagerTesting, ReusesSlotsRatherThanGrowing)
{
    std::vector<Handle> handles;
    for (int i = 0; i < 4; ++i) // exactly one block
    {
        handles.push_back(Open("client-" + std::to_string(i)));
    }
    const std::size_t capacity_when_full = manager.Capacity();

    for (const Handle handle : handles)
    {
        ASSERT_TRUE(manager.beginClose(handle));
    }
    for (int i = 0; i < 4; ++i)
    {
        Open("next-" + std::to_string(i));
    }

    EXPECT_EQ(manager.Capacity(), capacity_when_full);
    EXPECT_EQ(manager.Size(), 4u);
    EXPECT_EQ(FakeSession::live, 4);
}

// =============================================================================
// The full close sequence, end to end
// =============================================================================

TEST_F(SessionManagerTesting, AcancelledReadSettlesBeforeTheSessionGoes)
{
    // What a server does when it decides to drop a client that has a read
    // parked in the kernel.
    const Handle handle = Open("client-1");
    ASSERT_TRUE(manager.OnOperationSubmitted(handle)); // recv submitted

    // 1. The server wants the session gone.
    EXPECT_FALSE(manager.beginClose(handle));
    EXPECT_TRUE(manager.IsDraining(handle));

    // 2. It submits a cancellation. The read is still outstanding, so nothing
    //    may be destroyed yet - the kernel still owns the session's buffer.
    EXPECT_EQ(FakeSession::live, 1);
    EXPECT_EQ(FakeTask::live, 1);

    // 3. The cancellation is acknowledged: the read comes back as -EcancelED.
    EXPECT_EQ(manager.OnOperationSettled(handle), nullptr);

    // 4. Only now are the session, its buffers and its coroutine frame gone.
    EXPECT_EQ(FakeSession::live, 0);
    EXPECT_EQ(FakeTask::live, 0);
    EXPECT_FALSE(manager.Contains(handle));

    // 5. And a completion that still shows up afterwards resolves to nothing.
    EXPECT_EQ(manager.OnOperationSettled(handle), nullptr);
    EXPECT_EQ(manager.find(handle), nullptr);
}
