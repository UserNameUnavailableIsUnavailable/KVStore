#include <NBIO/Async/Scheduler.hpp>
#include <NBIO/Signal/SystemSignal.hpp>
#include <NBIO/Core/EpollMultiplexer.hpp>
#include <NBIO/Signal/SystemSignalChannel.hpp>
#include <iostream>
#include <thread>

using namespace NBIO;

int main(int argc, char* argv[]) {
    {
        NBIO::Core::EpollMultiplexer mux;
        Async::Scheduler sched([](bool) {});
        Signal::SystemSignal signal;
        NBIO::Signal::SystemSignalChannel svc(signal, mux, sched);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "1s elapsed" << std::endl;
    }
    return 0;
}


