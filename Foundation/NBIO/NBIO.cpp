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

std::unique_ptr<TcpAcceptChannel> bind(const Foundation::Core::SocketAddress &address, int backlog)
{
    Foundation::Core::TcpSocket socket(address.family(), Foundation::Core::TcpSocket::Type::kStream);
    if (auto result = socket.non_blocking(); !result)
    {
        throw std::system_error(result.error(), "TcpSocket: non_blocking failed");
    }
    if (auto result = socket.reuse_address(); !result) // enable address reuse for quick restart
    {
        throw std::system_error(result.error(), "TcpSocket: set_reuse_address failed");
    }
    if (auto result = socket.bind(address); !result)
    {
        throw std::system_error(result.error(), "TcpSocket: bind failed");
    }
    if (auto result = socket.listen(backlog); !result)
    {
        throw std::system_error(result.error(), "TcpSocket: listen failed");
    }
    return std::make_unique<TcpAcceptChannel>(std::move(socket), Engine::multiplexer(), Engine::scheduler());
}

std::shared_ptr<TcpSession> establish(Foundation::Core::TcpSocket socket)
{
    return std::make_shared<TcpSession>(std::move(socket), Engine::multiplexer(), Engine::scheduler());
}
} // namespace Foundation::NBIO
