#include <gtest/gtest.h>

#include <nbio/nbio.hpp>
#include <nbio/notification/ConditionVariable.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace {
using nbio::notification::ConditionVariable;
using nbio::runtime;

// A ConditionVariable binds the engine's notify channel on construction, so the
// runtime has to be installed before any test body runs.
class ConditionTesting : public ::testing::Test {};

nbio::Task<void> wait_for_flag(ConditionVariable& condition, std::atomic_bool& ready,
                                           std::atomic_int& resumed, std::promise<void>* first_resume = nullptr) {
    co_await condition.wait([&] { return ready.load(std::memory_order_acquire); });

    const int current = resumed.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (first_resume != nullptr && current == 1) {
        first_resume->set_value();
    }
}

// Parks forever: the predicate never holds, so the only way out is destruction.
nbio::Task<void> Park(ConditionVariable& condition, std::atomic_int& resumed) {
    co_await condition.wait([] { return false; });
    resumed.fetch_add(1, std::memory_order_acq_rel);
}
}  // namespace

TEST_F(ConditionTesting, WaitsUntilPredicateTurnsTrue) {
    ConditionVariable condition;
    std::atomic_bool ready{false};
    std::atomic_int resumed{0};

    auto token = runtime::scheduler().Spawn(wait_for_flag(condition, ready, resumed));
    runtime::scheduler().Run();

    std::thread notifier([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        ready.store(true, std::memory_order_release);
        condition.NotifyOne();
    });

    runtime::scheduler().Run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 1);
    EXPECT_TRUE(token.IsDead());
}

TEST_F(ConditionTesting, NotifyOneWakesOneWaiterAtATime) {
    ConditionVariable condition;
    std::atomic_bool ready{false};
    std::atomic_int resumed{0};
    std::promise<void> first_resume;
    auto first_resume_future = first_resume.get_future();

    auto first_token = runtime::scheduler().Spawn(wait_for_flag(condition, ready, resumed, &first_resume));
    auto second_token = runtime::scheduler().Spawn(wait_for_flag(condition, ready, resumed));
    runtime::scheduler().Run();

    std::thread notifier([&] {
        ready.store(true, std::memory_order_release);
        condition.NotifyOne();
        first_resume_future.wait();
        condition.NotifyOne();
    });

    runtime::scheduler().Run();
    EXPECT_EQ(resumed.load(std::memory_order_acquire), 1);

    runtime::scheduler().Run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 2);
    EXPECT_TRUE(first_token.IsDead());
    EXPECT_TRUE(second_token.IsDead());
}

TEST_F(ConditionTesting, NotifyAllWakesEveryWaiter) {
    ConditionVariable condition;
    std::atomic_bool ready{false};
    std::atomic_int resumed{0};

    auto first_token = runtime::scheduler().Spawn(wait_for_flag(condition, ready, resumed));
    auto second_token = runtime::scheduler().Spawn(wait_for_flag(condition, ready, resumed));
    auto third_token = runtime::scheduler().Spawn(wait_for_flag(condition, ready, resumed));
    runtime::scheduler().Run();

    std::thread notifier([&] {
        ready.store(true, std::memory_order_release);
        condition.NotifyAll();
    });

    runtime::scheduler().Run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 3);
    EXPECT_TRUE(first_token.IsDead());
    EXPECT_TRUE(second_token.IsDead());
    EXPECT_TRUE(third_token.IsDead());
}

