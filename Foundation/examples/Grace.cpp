#include <iostream>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/NBIO/NBIO.hpp>

using namespace Foundation;

NBIO::Task<void> Grace()
{
	co_await NBIO::wait_for_signal();
	co_await NBIO::sleep_for(std::chrono::seconds(2));
	std::cout << "Period of grace ends" << std::endl;
	exit(1);
}

int main(int argc, char* argv[])
{
	NBIO::spawn(Grace());
	Async::run([]() -> NBIO::Task<void> {
		co_await NBIO::sleep_for(std::chrono::seconds(10));
	}());
	std::cout << "The process exited gracefully" << std::endl;
	return 0;
}
