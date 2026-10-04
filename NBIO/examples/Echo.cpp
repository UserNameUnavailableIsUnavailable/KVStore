#include <NBIO/Async/Async.hpp>
#include <NBIO/Async/Task.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpSocket.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/NBIO.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <iostream>
#include <memory>

using namespace NBIO;

NBIO::Task<void> Service() {
    auto address = Net::SocketAddress::from_v4("127.0.0.1", 8080);
    NBIO::Net::TcpAcceptService acceptor{address};
    while (true) {
        auto accepted = co_await acceptor.accept();
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
        NBIO::spawn([](std::shared_ptr<NBIO::Net::TcpSessionService> session) -> NBIO::Task<void> {
            auto buffer = std::make_unique<::NBIO::Utility::Buffer>(1024);
            while (true) {
                {
                    auto res = co_await session->receive(buffer->writable_span());
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
                    auto res = co_await session->send(buffer->readable_span());
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

int main() { Async::run(Service()); }


