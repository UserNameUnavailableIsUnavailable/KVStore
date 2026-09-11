#include <iostream>
#include <memory>
#include <Foundation/Address.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Async/Engine.hpp>
#include <Foundation/Socket.hpp>

using namespace Foundation;

Async::Task<void> Service()
{
    auto address = Address::from_ipv4("127.0.0.1", 8080);
    auto listen_service = Async::Net::listen_on(address);
    while (true)
    {
        auto result = co_await listen_service->accept();
        auto session = Async::Net::establish_with(std::move(result.socket));
        // A coroutine lambda is fine, but: (1) Spawn takes a Task, so the
        // lambda must be INVOKED here; (2) captures live in the closure, not
        // the coroutine frame -- the temporary closure dies before the lazy
        // task ever runs, so anything it needs must arrive as a by-value
        // parameter (parameters are moved into the frame at call time).
        Async::spawn(
            [](std::shared_ptr<Async::Session> session) -> Async::Task<void> {
                auto buffer = std::make_unique<::Foundation::Buffer>(1024);
                while (true)
                {
                    {
                        auto res = co_await session->receive(*buffer);
                        if (res.status != ::ReceiveStatus::kDone)
                        {
                            std::cout << "[conn] closed\n";
                            co_return;
                        }
                        std::cout << "[conn] received " << res.bytes_transferred << " bytes: " << buffer->string_view() << std::endl;
                    }
                    {
                        auto res = co_await session->send(*buffer);
                        if (res.status != ::SendStatus::kDone)
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
