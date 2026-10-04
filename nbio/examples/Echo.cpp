#include <nbio/async/Async.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/utility/Buffer.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/TcpSocket.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/nbio.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <iostream>
#include <memory>

using namespace nbio;

nbio::Task<void> Service() {
    auto address = net::Address::FromV4("127.0.0.1", 8080);
    nbio::net::TcpAcceptService acceptor{address};
    while (true) {
        auto accepted = co_await acceptor.Accept();
        if (!accepted) {
            std::cout << "[conn] failed\n";
            co_return;
        }
        auto session = std::move(accepted->first);
        // A coroutine lambda is fine, but: (1) Spawn takes a Task, so the
        // lambda must be INVOKED here; (2) captures live in the closure, not
        // the coroutine frame -- the temporary closure dies before the lazy
        // task ever runs, so anything it needs must arrive as a by-value
        // parameter (parameters are moved into the frame at call time).
        nbio::Spawn([](std::shared_ptr<nbio::net::TcpSessionService> session) -> nbio::Task<void> {
            auto buffer = std::make_unique<::nbio::utility::Buffer>(1024);
            while (true) {
                {
                    auto res = co_await session->Receive(buffer->writable_span());
                    if (!res) {
                        std::cout << "[conn] failed\n";
                        co_return;
                    }
                    if (*res == 0) {
                        std::cout << "[conn] peer closed\n";
                        co_return;
                    }
                    buffer->commit(*res);
                    std::cout << "[conn] received " << *res << " bytes: " << buffer->string_view() << std::endl;
                }
                {
                    auto res = co_await session->Send(buffer->readable_span());
                    if (!res) {
                        std::cout << "[conn] send failed\n";
                        co_return;
                    }
                    if (res == 0) {
                        std::cout << "[conn] peer closed\n";
                        co_return;
                    }
                    buffer->consume(*res);
                    std::cout << "[conn] sent " << buffer->string_view() << std::endl;
                    buffer->consume_all();
                }
            }
        }(std::move(session)));
    }
}

int main() { async::run(Service()); }


