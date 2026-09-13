#include <iostream>
#include <Foundation/NBIO/Runtime.hpp>
#include <memory>
#include <Foundation/Core/Address.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <Foundation/NBIO/NBIO.hpp>

using namespace Foundation;

NBIO::Task<void> Service()
{
    auto address = Core::Address::from_ipv4("127.0.0.1", 8080);
    auto listen_service = NBIO::listen_on(address);
    while (true)
    {
        auto result = co_await listen_service->accept();
        auto session = NBIO::establish_with(std::move(result.socket));
        // A coroutine lambda is fine, but: (1) Spawn takes a Task, so the
        // lambda must be INVOKED here; (2) captures live in the closure, not
        // the coroutine frame -- the temporary closure dies before the lazy
        // task ever runs, so anything it needs must arrive as a by-value
        // parameter (parameters are moved into the frame at call time).
        NBIO::spawn(
            [](std::shared_ptr<NBIO::Session> session) -> NBIO::Task<void> {
                auto buffer = std::make_unique<::Foundation::Core::Buffer>(1024);
                while (true)
                {
                    {
                        auto res = co_await session->receive(*buffer);
                        if (res.status != Core::ReceiveStatus::kDone)
                        {
                            std::cout << "[conn] closed\n";
                            co_return;
                        }
                        std::cout << "[conn] received " << res.bytes_transferred << " bytes: " << buffer->string_view() << std::endl;
                    }
                    {
                        auto res = co_await session->send(*buffer);
                        if (res.status != Core::SendStatus::kDone)
                        {
                            std::cout << "[conn] send failed\n";
                            co_return;
                        }
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
