#include <nbio/async/Scheduler.hpp>
#include <nbio/signal/SystemSignal.hpp>
#include <nbio/core/EpollMultiplexer.hpp>
#include <nbio/signal/SystemSignalChannel.hpp>
#include <iostream>
#include <thread>

using namespace nbio;

int main(int argc, char* argv[]) {
    {
        nbio::core::EpollMultiplexer mux;
        async::Scheduler sched([](bool) {});
        signal::SystemSignal signal;
        nbio::signal::SystemSignalChannel svc(signal, mux, sched);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "1s elapsed" << std::endl;
    }
    return 0;
}


