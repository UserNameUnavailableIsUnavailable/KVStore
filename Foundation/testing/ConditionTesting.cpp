#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include <Foundation/NBIO/ConditionVariable.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

namespace
{
using Foundation::NBIO::ConditionVariable;
using Foundation::NBIO::Engine;

// A ConditionVariable binds the engine's notify channel on construction, so the
// runtime has to be installed before any test body runs.
class ConditionTesting : public ::testing::Test
{
};

Foundation::NBIO::Task<void> wait_for_flag(ConditionVariable &condition, std::atomic_bool &ready,
                                           std::atomic_int &resumed, std::promise<void> *first_resume = nullptr)
{
    co_await condition.wait([&] {
        return ready.load(std::memory_order_acquire);
    });

    const int current = resumed.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (first_resume != nullptr && current == 1)
    {
        first_resume->set_value();
    }
}

// Parks forever: the predicate never holds, so the only way out is destruction.
Foundation::NBIO::Task<void> park(ConditionVariable &condition, std::atomic_int &resumed)
{
    co_await condition.wait([] {
        return false;
    });
    resumed.fetch_add(1, std::memory_order_acq_rel);
}
} // namespace

TEST_F(ConditionTesting, WaitsUntilPredicateTurnsTrue)
{
    ConditionVariable condition;
    std::atomic_bool ready{false};
    std::atomic_int resumed{0};

    auto token = Engine::scheduler().spawn(wait_for_flag(condition, ready, resumed));
    Engine::scheduler().run();

    std::thread notifier([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        ready.store(true, std::memory_order_release);
        condition.notify_one();
    });

    Engine::scheduler().run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 1);
    EXPECT_TRUE(token.is_finished());
}

TEST_F(ConditionTesting, NotifyOneWakesOneWaiterAtATime)
{
    ConditionVariable condition;
    std::atomic_bool ready{false};
    std::atomic_int resumed{0};
    std::promise<void> first_resume;
    auto first_resume_future = first_resume.get_future();

    auto first_token = Engine::scheduler().spawn(wait_for_flag(condition, ready, resumed, &first_resume));
    auto second_token = Engine::scheduler().spawn(wait_for_flag(condition, ready, resumed));
    Engine::scheduler().run();

    std::thread notifier([&] {
        ready.store(true, std::memory_order_release);
        condition.notify_one();
        first_resume_future.wait();
        condition.notify_one();
    });

    Engine::scheduler().run();
    EXPECT_EQ(resumed.load(std::memory_order_acquire), 1);

    Engine::scheduler().run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 2);
    EXPECT_TRUE(first_token.is_finished());
    EXPECT_TRUE(second_token.is_finished());
}

TEST_F(ConditionTesting, NotifyAllWakesEveryWaiter)
{
    ConditionVariable condition;
    std::atomic_bool ready{false};
    std::atomic_int resumed{0};

    auto first_token = Engine::scheduler().spawn(wait_for_flag(condition, ready, resumed));
    auto second_token = Engine::scheduler().spawn(wait_for_flag(condition, ready, resumed));
    auto third_token = Engine::scheduler().spawn(wait_for_flag(condition, ready, resumed));
    Engine::scheduler().run();

    std::thread notifier([&] {
        ready.store(true, std::memory_order_release);
        condition.notify_all();
    });

    Engine::scheduler().run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 3);
    EXPECT_TRUE(first_token.is_finished());
    EXPECT_TRUE(second_token.is_finished());
    EXPECT_TRUE(third_token.is_finished());
}