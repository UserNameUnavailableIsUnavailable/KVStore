#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Signal/SystemSignal.hpp>
#include <NBIO/Core/EpollMultiplexer.hpp>
#include <NBIO/Signal/SystemSignalChannel.hpp>
#include <iostream>
#include <thread>

using namespace NBIO;

// SystemSignalChannel intercepts signals.
// The user is responsible to deal with signals.
// This example shows how Async::wait_for_signal works.

int main(int argc, char* argv[]) {
    {
        NBIO::Core::EpollMultiplexer mux;
        Async::Scheduler sched([](bool) {});
        // creating SystemSignalChannel will intercept signals
        Signal::SystemSignal signal;
        NBIO::Signal::SystemSignalChannel svc(signal, mux, sched);
        // the process won't exit within 2 seconds
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::cout << "2s elapsed" << std::endl;
    }
    std::cout << "The process is exitting normally" << std::endl;
    return 0;
}


