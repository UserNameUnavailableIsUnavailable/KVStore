#include <nbio/async/Async.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/nbio.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <chrono>
#include <iostream>

using namespace nbio;

nbio::Task<void> Sleep() {
    std::cout << "Sleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    co_await nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(1));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count()
              << "ms" << std::endl;
}

nbio::Task<void> AllSleep() {
    std::cout << "AllSleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    auto sleep1 = nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(1));
    auto sleep2 = nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(2));
    auto sleep3 = nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(3));
    co_await async::WhenAll(std::move(sleep1), std::move(sleep2), std::move(sleep3));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count()
              << "ms" << std::endl;
}

nbio::Task<void> AnySleep() {
    std::cout << "AnySleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    auto sleep1 = nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(1));
    auto sleep2 = nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(2));
    auto sleep3 = nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(3));
    co_await async::WhenAny(std::move(sleep1), std::move(sleep2), std::move(sleep3));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count()
              << "ms" << std::endl;
}

int main() {
    nbio::run([]() -> nbio::Task<void> {
        co_await Sleep();
        co_await AllSleep();
        co_await AnySleep();
    }());
}


