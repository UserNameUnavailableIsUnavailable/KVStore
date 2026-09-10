#include <iostream>
#include <Foundation/Async/Async.hpp>

using namespace Foundation;

Async::Task<void> Grace()
{
	co_await Async::wait_for_signal();
	co_await Async::sleep_for(std::chrono::seconds(2));
	std::cout << "Period of grace ends" << std::endl;
	exit(1);
}

int main(int argc, char* argv[])
{
	Async::spawn(Grace());
	Async::run([]() -> Async::Task<void> {
		co_await Async::sleep_for(std::chrono::seconds(10));
	}());
	std::cout << "The process exited gracefully" << std::endl;
	return 0;
}
