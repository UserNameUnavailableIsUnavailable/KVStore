#include <Foundation/Async/SignalService.hpp>
#include <Foundation/Async/EpollMultiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>

#include <iostream>
#include <thread>

using namespace Foundation;

int main(int argc, char* argv[])
{
	{
		Async::EpollMultiplexer mux;
		Async::Scheduler sched([](bool){ });
		Async::SignalService svc(mux, sched);
		std::this_thread::sleep_for(std::chrono::seconds(1));
		std::cout << "1s elapsed" << std::endl;
	}
	return 0;
}
