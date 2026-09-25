#include <iostream>
#include <Foundation/NBIO/Runtime.hpp>
#include <memory>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/TcpSocket.hpp>
#include <Foundation/NBIO/NBIO.hpp>

using namespace Foundation;

NBIO::Task<void> Service()
{
    auto address = Core::SocketAddress::from_v4("127.0.0.1", 8080);
    NBIO::TcpAcceptService acceptor{address};
    while (true)
    {
        auto accepted = co_await acceptor.accept();
        if (!accepted)
        {
            std::cout << "[conn] failed\n";
            co_return;
        }
        auto session = std::move(accepted->first);
        // A coroutine lambda is fine, but: (1) Spawn takes a Task, so the
        // lambda must be INVOKED here; (2) captures live in the closure, not
        // the coroutine frame -- the temporary closure dies before the lazy
        // task ever runs, so anything it needs must arrive as a by-value
        // parameter (parameters are moved into the frame at call time).
        NBIO::spawn(
            [](std::shared_ptr<NBIO::TcpSessionService> session) -> NBIO::Task<void> {
                auto buffer = std::make_unique<::Foundation::Core::Buffer>(1024);
                while (true)
                {
                    {
                        auto res = co_await session->receive(buffer->writable_span());
                        if (!res)
                        {
                            std::cout << "[conn] failed\n";
                            co_return;
                        }
                        if (*res == 0)
                        {
                            std::cout << "[conn] peer closed\n";
                            co_return;
                        }
                        buffer->commit(*res);
                        std::cout << "[conn] received " << *res << " bytes: " << buffer->string_view() << std::endl;
                    }
                    {
                        auto res = co_await session->send(buffer->readable_span());
                        if (!res)
                        {
                            std::cout << "[conn] send failed\n";
                            co_return;
                        }
                        if (res == 0)
                        {
                            std::cout << "[conn] peer closed\n";
                            co_return;
                        }
                        buffer->consume(*res);
                        std::cout << "[conn] sent " << buffer->string_view() << std::endl;
                        buffer->consume_all();
                    }
                }
            }(std::move(session))
        );
    }
}

int main()
{
    Async::run(Service());
}
