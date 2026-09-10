#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Async/Condition.hpp>
#include <Foundation/Async/URingMultiplexer.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <thread>

std::atomic_bool ok{ false };

using namespace Foundation;

Async::Task<void> wait_condition(Async::Condition& condition)
{
    co_await condition.wait([] {
        return ok.load(std::memory_order_acquire);
    });
    std::cout << "condition satisfied" << std::endl;
}

int main()
{
    auto multiplexer = std::make_unique<Async::URingMultiplexer>();
    Async::use_multiplexer(std::move(multiplexer));
    Async::Condition condition;
    std::future<void> task = std::async([&condition] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        ok.store(true, std::memory_order_release);
        std::cout << "sleep finished" << std::endl;
        condition.notify_one();
    });
    ::Async::run(wait_condition(condition));
}