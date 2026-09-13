#include <Foundation/NBIO/NBIO.hpp>

#include "Engine.hpp"

namespace Foundation::NBIO
{
void initialize(std::unique_ptr<Multiplexer> multiplexer)
{
    Engine::initialize(std::move(multiplexer));
}

void run(Task<void> main)
{
    Foundation::Async::run(std::move(main));
}

Task<void> sleep_until(std::chrono::steady_clock::time_point time_point)
{
    co_await Engine::timer_channel().sleep(time_point);
}

Task<void> sleep_for(std::chrono::steady_clock::duration duration)
{
    co_await Engine::timer_channel().sleep(std::chrono::steady_clock::now() + duration);
}

Task<void> wait_for_signal()
{
    co_await Engine::signal_channel().wait();
}

std::shared_ptr<FileStream> open_file(const std::filesystem::path &p)
{
    return FileStream::Open(p.string(), Foundation::Core::FileMode::kReadWrite | Foundation::Core::FileMode::kCreate, 0644,
                            Engine::multiplexer(), Engine::scheduler());
}

std::unique_ptr<ListenChannel> listen_on(const Foundation::Core::Address &address, int backlog)
{
    Foundation::Core::Socket socket(address.family(), Foundation::Core::Socket::Type::kStream);
    socket.set_non_blocking();
    socket.set_reuse_address(); // enable address reuse for quick restart
    socket.bind(address);
    socket.listen(backlog);
    return std::make_unique<ListenChannel>(std::move(socket), Engine::multiplexer(), Engine::scheduler());
}

std::shared_ptr<Session> establish_with(Foundation::Core::Socket socket)
{
    return std::make_shared<Session>(std::move(socket), Engine::multiplexer(), Engine::scheduler());
}
} // namespace Foundation::NBIO
