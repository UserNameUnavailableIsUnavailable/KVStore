#include <chrono>
#include <Foundation/NBIO/Runtime.hpp>
#include <iostream>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/NBIO/NBIO.hpp>

using namespace Foundation;

NBIO::Task<void> Sleep()
{
	std::cout << "Sleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    co_await NBIO::SystemTimeService{}.sleep(std::chrono::seconds(1));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count() << "ms" << std::endl;
}

NBIO::Task<void> AllSleep()
{
	std::cout << "AllSleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    auto sleep1 = NBIO::SystemTimeService{}.sleep(std::chrono::seconds(1));
    auto sleep2 = NBIO::SystemTimeService{}.sleep(std::chrono::seconds(2));
    auto sleep3 = NBIO::SystemTimeService{}.sleep(std::chrono::seconds(3));
    co_await Async::when_all(std::move(sleep1), std::move(sleep2), std::move(sleep3));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count() << "ms" << std::endl;
}

NBIO::Task<void> AnySleep()
{
	std::cout << "AnySleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    auto sleep1 = NBIO::SystemTimeService{}.sleep(std::chrono::seconds(1));
    auto sleep2 = NBIO::SystemTimeService{}.sleep(std::chrono::seconds(2));
    auto sleep3 = NBIO::SystemTimeService{}.sleep(std::chrono::seconds(3));
    co_await Async::when_any(std::move(sleep1), std::move(sleep2), std::move(sleep3));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count() << "ms" << std::endl;
}

int main()
{
    NBIO::run([]() -> NBIO::Task<void>{
		co_await Sleep();
		co_await AllSleep();
		co_await AnySleep();
	}());
}
