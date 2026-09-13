#include <Foundation/NBIO/SignalChannel.hpp>
#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Core/Signal.hpp>

#include <iostream>
#include <thread>

using namespace Foundation;

// SignalChannel intercepts signals.
// The user is responsible to deal with signals.
// This example shows how Async::wait_for_signal works.

int main(int argc, char* argv[])
{
	{
		NBIO::EpollMultiplexer mux;
		Async::Scheduler sched([](bool){ });
		// creating SignalChannel will intercept signals
		Core::Signal signal;
		NBIO::SignalChannel svc(signal, mux, sched);
		// the process won't exit within 2 seconds
		std::this_thread::sleep_for(std::chrono::seconds(2));
		std::cout << "2s elapsed" << std::endl;
	}
	std::cout << "The process is exitting normally" << std::endl;
	return 0;
}
