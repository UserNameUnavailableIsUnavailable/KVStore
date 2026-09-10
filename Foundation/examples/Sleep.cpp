#include <chrono>
#include <iostream>
#include <Foundation/Async/Engine.hpp>
#include <Foundation/Async/Async.hpp>

using namespace Foundation;

Async::Task<void> Sleep()
{
	std::cout << "Sleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    co_await Async::sleep_for(std::chrono::seconds(1));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count() << "ms" << std::endl;
}

Async::Task<void> AllSleep()
{
	std::cout << "AllSleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    auto sleep1 = Async::sleep_for(std::chrono::seconds(1));
    auto sleep2 = Async::sleep_for(std::chrono::seconds(2));
    auto sleep3 = Async::sleep_for(std::chrono::seconds(3));
    co_await Async::when_all(std::move(sleep1), std::move(sleep2), std::move(sleep3));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count() << "ms" << std::endl;
}

Async::Task<void> AnySleep()
{
	std::cout << "AnySleep ";
    std::chrono::steady_clock::time_point before = std::chrono::steady_clock::now();
    auto sleep1 = Async::sleep_for(std::chrono::seconds(1));
    auto sleep2 = Async::sleep_for(std::chrono::seconds(2));
    auto sleep3 = Async::sleep_for(std::chrono::seconds(3));
    co_await Async::when_any(std::move(sleep1), std::move(sleep2), std::move(sleep3));
    std::chrono::steady_clock::time_point after = std::chrono::steady_clock::now();
    std::cout << "slept for: " << std::chrono::duration_cast<std::chrono::milliseconds>((after - before)).count() << "ms" << std::endl;
}

int main()
{
    Async::run([]() -> Async::Task<void>{
		co_await Sleep();
		co_await AllSleep();
		co_await AnySleep();
	}());
}
