#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Condition.hpp>

#include "Engine.hpp"

namespace Foundation::Async
{
void run(Task<void> main)
{
    detail::Engine::run(std::move(main));
}
Task<void> sleep_until(std::chrono::steady_clock::time_point time_point)
{
    co_await detail::Engine::instance().timer_service().SleepUntil(time_point);
}
Task<void> sleep_for(std::chrono::steady_clock::duration duration)
{
    co_await detail::Engine::instance().timer_service().SleepFor(duration);
}
Task<void> wait_for_signal()
{
    co_await detail::Engine::instance().signal_service().wait();
}

Condition::Condition() :
    condition_(detail::Engine::instance().notify_service().channel())
{
}

Condition::~Condition() = default;

namespace IO
{
std::shared_ptr<FileStream> open_file(const std::filesystem::path &p)
{
    auto &engine = detail::Engine::instance();
    return FileStream::Open(p.string(), Foundation::FileMode::kReadWrite | Foundation::FileMode::kCreate, 0644,
                            engine.multiplexer(), engine.scheduler());
}
} // namespace IO

// Non-blocking IO
namespace Net
{
std::unique_ptr<ListenService> listen_on(const Foundation::Address &address, int backlog)
{
    auto &engine = detail::Engine::instance();
    return std::make_unique<ListenService>(address, engine.multiplexer(), engine.scheduler(), backlog);
}

std::shared_ptr<Session> establish_with(Socket socket)
{
    auto &engine = detail::Engine::instance();
    auto &scheduler = engine.scheduler();
    auto session = std::make_shared<Session>(std::move(socket), engine.multiplexer(), scheduler);
    return session;
}
} // namespace Net
} // namespace Foundation::Async
