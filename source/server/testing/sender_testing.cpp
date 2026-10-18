#include <gtest/gtest.h>

#include <nbio/async.hpp>
#include <nbio/core/epoll_multiplexer.hpp>
#include <nbio/net/tcp_listener.hpp>
#include <resp/sender.hpp>

#include <array>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>

namespace {
class RESPSending : public testing::Test {
   protected:
    void SetUp() override {
        if (!nbio::runtime::Daemon::is_initialized()) {
            nbio::runtime::Daemon::Initialize(std::make_unique<nbio::core::EpollMultiplexer>());
        }
        const auto bind_address = nbio::net::Address::FromIPv4("127.0.0.1", 0);
        ASSERT_TRUE(bind_address);
        ASSERT_TRUE(listener_.Bind(*bind_address));
        const auto address = listener_.GetLocalAddress();
        ASSERT_TRUE(address);
        ASSERT_TRUE(client_.Connect(*address));
        auto accepted = listener_.Accept();
        ASSERT_TRUE(accepted);
        session_ = std::make_unique<nbio::net::TcpSessionService>(std::move(*accepted));
        const timeval timeout{5, 0};
        ASSERT_EQ(::setsockopt(static_cast<int>(client_.native_handle()), SOL_SOCKET, SO_RCVTIMEO,
                               &timeout, sizeof(timeout)), 0);
    }

    static nbio::async::Task<void> Flush(RESP::Sender& sender, bool& sent) {
        sent = co_await sender.Flush();
    }

    std::string Read(std::size_t size) {
        std::string bytes;
        std::array<char, 16384> buffer{};
        while (bytes.size() < size) {
            auto result = client_.Receive(buffer);
            if (!result) {
                throw std::system_error(result.error(), "reading test replies");
            }
            if (*result == 0) {
                throw std::runtime_error("connection closed before all test replies arrived");
            }
            bytes.append(buffer.data(), *result);
        }
        return bytes;
    }

    nbio::net::TcpListener listener_;
    nbio::net::TcpConnector client_;
    std::unique_ptr<nbio::net::TcpSessionService> session_;
};

TEST_F(RESPSending, FlushPreservesBatchedReplyOrderAndReusesTheBuffer) {
    nbio::utility::Buffer buffer;
    RESP::Sender sender(*session_, buffer);
    bool sent = false;
    nbio::async::Run(Flush(sender, sent));
    ASSERT_TRUE(sent);
    EXPECT_TRUE(buffer.is_empty());
    for (int iteration = 0; iteration < 3; ++iteration) {
        sender.Append(RESP::Object(RESP::SimpleString{.value = "OK"}));
        sender.Append(RESP::Object(RESP::BulkString{.value = std::string("a\0b", 3)}));
        nbio::async::Run(Flush(sender, sent));
        ASSERT_TRUE(sent);
        EXPECT_TRUE(buffer.is_empty());
        EXPECT_EQ(Read(14), std::string("+OK\r\n$3\r\na\0b\r\n", 14));
    }
}

TEST_F(RESPSending, FlushHandlesPartialSendsAndBackpressureWithoutLosingBytes) {
    const int capacity = 4096;
    ASSERT_EQ(::setsockopt(static_cast<int>(session_->connector().native_handle()), SOL_SOCKET, SO_SNDBUF,
                           &capacity, sizeof(capacity)), 0);
    const std::string value(4U * 1024U * 1024U, 'v');
    const std::string expected = "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n+OK\r\n";
    nbio::utility::Buffer buffer(16384, 8U * 1024U * 1024U);
    RESP::Sender sender(*session_, buffer);
    sender.Append(RESP::Object(RESP::BulkString{.value = value}));
    sender.Append(RESP::Object(RESP::SimpleString{.value = "OK"}));
    auto reader = std::async(std::launch::async, [this, size = expected.size()] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return Read(size);
    });
    bool sent = false;
    nbio::async::Run(Flush(sender, sent));
    EXPECT_TRUE(sent) << sender.internal_error();
    EXPECT_TRUE(buffer.is_empty());
    EXPECT_EQ(reader.get(), expected);
}

TEST_F(RESPSending, FlushReportsAnUnusableSocket) {
    session_->connector().Close();
    nbio::utility::Buffer buffer;
    RESP::Sender sender(*session_, buffer);
    sender.Append(RESP::Object(RESP::SimpleString{.value = "OK"}));
    bool sent = true;
    nbio::async::Run(Flush(sender, sent));
    EXPECT_FALSE(sent);
    EXPECT_FALSE(sender.internal_error().empty());
}
}  // namespace
