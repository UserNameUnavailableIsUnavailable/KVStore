#include <Foundation/Async/Async.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/ConditionVariable.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <thread>

std::atomic_bool ok{ false };

using namespace Foundation;

NBIO::Task<void> wait_condition(NBIO::ConditionVariable& cv)
{
    co_await cv.wait([] {
        return ok.load(std::memory_order_acquire);
    });
    std::cout << "condition satisfied" << std::endl;
}

int main()
{
    auto multiplexer = std::make_unique<NBIO::URingMultiplexer>();
    NBIO::initialize(std::move(multiplexer));
    NBIO::ConditionVariable condition;
    std::future<void> task = std::async([&condition] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ok.store(true, std::memory_order_release);
        std::cout << "sleep finished" << std::endl;
        condition.notify_one();
    });
    NBIO::run(wait_condition(condition));
}
