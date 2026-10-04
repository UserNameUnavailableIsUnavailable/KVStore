#include <nbio/async/Scheduler.hpp>
#include <nbio/signal/SystemSignal.hpp>
#include <nbio/core/EpollMultiplexer.hpp>
#include <nbio/signal/SystemSignalChannel.hpp>
#include <iostream>
#include <thread>

using namespace nbio;

// SystemSignalChannel intercepts signals.
// The user is responsible to deal with signals.
// This example shows how async::wait_for_signal works.

int main(int argc, char* argv[]) {
    {
        nbio::core::EpollMultiplexer mux;
        async::Scheduler sched([](bool) {});
        // creating SystemSignalChannel will intercept signals
        signal::SystemSignal signal;
        nbio::signal::SystemSignalChannel svc(signal, mux, sched);
        // the process won't exit within 2 seconds
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::cout << "2s elapsed" << std::endl;
    }
    std::cout << "The process is exitting normally" << std::endl;
    return 0;
}


