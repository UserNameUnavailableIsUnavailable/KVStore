#include <Foundation/NBIO/SignalChannel.hpp>
#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Core/Signal.hpp>

#include <iostream>
#include <thread>

using namespace Foundation;

int main(int argc, char* argv[])
{
	{
		NBIO::EpollMultiplexer mux;
		Async::Scheduler sched([](bool){ });
		Core::Signal signal;
		NBIO::SignalChannel svc(signal, mux, sched);
		std::this_thread::sleep_for(std::chrono::seconds(1));
		std::cout << "1s elapsed" << std::endl;
	}
	return 0;
}
