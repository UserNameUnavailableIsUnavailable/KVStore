#include <NBIO/Async/Async.hpp>
#include <NBIO/NBIO.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <iostream>

using namespace NBIO;

NBIO::Task<void> Grace() {
    co_await NBIO::Signal::SystemSignalService{}.wait();
    co_await NBIO::Time::SystemTimeService{}.sleep(std::chrono::seconds(2));
    std::cout << "Period of grace ends" << std::endl;
    exit(1);
}

int main(int argc, char* argv[]) {
    NBIO::spawn(Grace());
    Async::run([]() -> NBIO::Task<void> { co_await NBIO::Time::SystemTimeService{}.sleep(std::chrono::seconds(10)); }());
    std::cout << "The process exited gracefully" << std::endl;
    return 0;
}


