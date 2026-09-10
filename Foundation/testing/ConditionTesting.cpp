#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include <Foundation/Async/Condition.hpp>
#include <Foundation/Async/Engine.hpp>
#include <Foundation/Async/Task.hpp>

namespace
{
using Foundation::Async::detail::Condition;

Foundation::Async::Task<void> wait_for_flag(Condition &condition,
                                std::atomic_bool &ready,
                                std::atomic_int &resumed,
                                std::promise<void> *first_resume = nullptr)
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
} // namespace

TEST(ConditionTesting, WaitsUntilPredicateTurnsTrue)
{
    auto &engine = Foundation::Async::detail::Engine::instance();
    auto &condition_channel = engine.notify_service().channel();
    Condition condition(condition_channel);

    std::atomic_bool ready{false};
    std::atomic_int resumed{0};

    auto token = engine.scheduler().spawn(wait_for_flag(condition, ready, resumed));
    engine.scheduler().run();

    std::thread notifier([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        ready.store(true, std::memory_order_release);
        condition.notify_one();
    });

    engine.scheduler().run();

    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 1);
    EXPECT_TRUE(token.is_finished());
}

TEST(ConditionTesting, NotifyOneWakesOneWaiterAtATime)
{
    auto &engine = Foundation::Async::detail::Engine::instance();
    auto &condition_channel = engine.notify_service().channel();
    Condition condition(condition_channel);

    std::atomic_bool ready{false};
    std::atomic_int resumed{0};
    std::promise<void> first_resume;
    auto first_resume_future = first_resume.get_future();

    auto first_token = engine.scheduler().spawn(wait_for_flag(condition, ready, resumed, &first_resume));
    auto second_token = engine.scheduler().spawn(wait_for_flag(condition, ready, resumed));
    engine.scheduler().run();

    std::thread notifier([&] {
        ready.store(true, std::memory_order_release);
        condition.notify_one();
        first_resume_future.wait();
        condition.notify_one();
    });

    engine.scheduler().run();
    EXPECT_EQ(resumed.load(std::memory_order_acquire), 1);

    engine.scheduler().run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 2);
    EXPECT_TRUE(first_token.is_finished());
    EXPECT_TRUE(second_token.is_finished());
}

TEST(ConditionTesting, NotifyAllWakesEveryWaiter)
{
    auto &engine = Foundation::Async::detail::Engine::instance();
    auto &condition_channel = engine.notify_service().channel();
    Condition condition(condition_channel);

    std::atomic_bool ready{false};
    std::atomic_int resumed{0};

    auto first_token = engine.scheduler().spawn(wait_for_flag(condition, ready, resumed));
    auto second_token = engine.scheduler().spawn(wait_for_flag(condition, ready, resumed));
    auto third_token = engine.scheduler().spawn(wait_for_flag(condition, ready, resumed));
    engine.scheduler().run();

    std::thread notifier([&] {
        ready.store(true, std::memory_order_release);
        condition.notify_all();
    });

    engine.scheduler().run();
    notifier.join();

    EXPECT_EQ(resumed.load(std::memory_order_acquire), 3);
    EXPECT_TRUE(first_token.is_finished());
    EXPECT_TRUE(second_token.is_finished());
    EXPECT_TRUE(third_token.is_finished());
}