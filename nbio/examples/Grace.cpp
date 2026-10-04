#include <nbio/async/Async.hpp>
#include <nbio/nbio.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <iostream>

using namespace nbio;

nbio::Task<void> Grace() {
    co_await nbio::signal::SystemSignalService{}.wait();
    co_await nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(2));
    std::cout << "Period of grace ends" << std::endl;
    exit(1);
}

int main(int argc, char* argv[]) {
    nbio::Spawn(Grace());
    async::run([]() -> nbio::Task<void> { co_await nbio::time::SystemTimeService{}.sleep(std::chrono::seconds(10)); }());
    std::cout << "The process exited gracefully" << std::endl;
    return 0;
}


