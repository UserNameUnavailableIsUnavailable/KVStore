// The socket channels take a queue the same way the file channels do, and what is
// particular to a socket is that its operations are not one per waiter: a
// readiness event can mean several connections, one sendmsg can carry several
// senders' bytes, and one readv can fill several receivers' buffers.
//
// These tests put several coroutines on one channel at once and check that each of
// them is answered and that the order a stream needs is kept.
#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/TcpSession.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/TcpSocket.hpp>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace
{
constexpr std::size_t kWriters = 4;
constexpr std::size_t kChunks = 8;
constexpr std::size_t kChunkBytes = 512;

// A fixed port: the tests run one at a time (each in its own process) and the
// listener is closed when the test ends.
constexpr std::uint16_t kTestPort = 34567;

Foundation::Core::SocketAddress TestSocketAddress()
{
    return Foundation::Core::SocketAddress::from_v4("127.0.0.1", kTestPort);
}

// The bytes one writer's chunk turns into, so the test and the writer agree on what
// was sent without either of them counting by hand.
std::string ChunkBytes(std::size_t writer, std::size_t index)
{
    return "chunk-" + std::to_string(writer) + ":" + std::to_string(index) + ";";
}

Foundation::NBIO::Task<void> send_chunks(std::shared_ptr<Foundation::NBIO::TcpSession> session, std::size_t writer)
{
    for (std::size_t index = 0; index < kChunks; ++index)
    {
        const std::string bytes = ChunkBytes(writer, index);
        (void)co_await session->send(std::span<const char>{bytes.data(), bytes.size()});
    }
}

Foundation::NBIO::Task<void> receive_chunk(std::shared_ptr<Foundation::NBIO::TcpSession> session, std::vector<std::string> &into,
                                           std::size_t index, std::size_t size)
{
    std::string bytes(size, '\0');
    const auto received = co_await session->receive(std::span<char>{bytes.data(), bytes.size()});
    bytes.resize(received.value_or(0));
    into[index] = std::move(bytes);
}

// Connects `count` clients, which sit in the listener's backlog until somebody
// accepts them.
std::vector<Foundation::Core::TcpSocket> connect_clients(const Foundation::Core::SocketAddress &address, std::size_t count)
{
    std::vector<Foundation::Core::TcpSocket> clients;
    for (std::size_t index = 0; index < count; ++index)
    {
        auto socket = Foundation::Core::TcpSocket{address.family(), Foundation::Core::TcpSocket::Type::kStream};
        (void)socket.connect(address);
        clients.push_back(std::move(socket));
    }
    return clients;
}

Foundation::NBIO::Task<void> accept_one(Foundation::NBIO::TcpAcceptChannel &acceptor, std::size_t &accepted)
{
    auto connection = co_await acceptor.accept();
    if (connection)
    {
        ++accepted;
    }
}

Foundation::NBIO::Task<void> accept_all(Foundation::NBIO::TcpAcceptChannel &acceptor, std::size_t &accepted)
{
    std::vector<Foundation::Async::CoroutineToken> waiters;
    waiters.reserve(kWriters);
    for (std::size_t index = 0; index < kWriters; ++index)
    {
        waiters.push_back(Foundation::NBIO::spawn(accept_one(acceptor, accepted)));
    }
    for (const Foundation::Async::CoroutineToken &waiter : waiters)
    {
        co_await waiter;
    }
}

Foundation::NBIO::Task<void> accept_and_send(Foundation::NBIO::TcpAcceptChannel &acceptor)
{
    auto connection = co_await acceptor.accept();
    if (!connection)
    {
        co_return;
    }
    auto session = Foundation::NBIO::establish(std::move(connection->first));

    std::vector<Foundation::Async::CoroutineToken> writers;
    writers.reserve(kWriters);
    for (std::size_t writer = 0; writer < kWriters; ++writer)
    {
        writers.push_back(Foundation::NBIO::spawn(send_chunks(session, writer)));
    }
    for (const Foundation::Async::CoroutineToken &writer : writers)
    {
        co_await writer;
    }
}

Foundation::NBIO::Task<void> accept_and_receive(Foundation::NBIO::TcpAcceptChannel &acceptor,
                                                Foundation::Core::TcpSocket &client, const std::string &sent,
                                                std::vector<std::string> &received, std::size_t bytes_per_receiver)
{
    auto connection = co_await acceptor.accept();
    if (!connection)
    {
        co_return;
    }
    auto session = Foundation::NBIO::establish(std::move(connection->first));

    std::vector<Foundation::Async::CoroutineToken> waiters;
    waiters.reserve(received.size());
    for (std::size_t index = 0; index < received.size(); ++index)
    {
        waiters.push_back(Foundation::NBIO::spawn(receive_chunk(session, received, index, bytes_per_receiver)));
    }

    co_await Foundation::NBIO::sleep_for(std::chrono::milliseconds(20));
    const auto sent_result = client.send(std::span<const char>{sent.data(), sent.size()});
    EXPECT_TRUE(sent_result);
    EXPECT_EQ(*sent_result, sent.size());

    for (const Foundation::Async::CoroutineToken &waiter : waiters)
    {
        co_await waiter;
    }
}
} // namespace

TEST(TcpSocketChannelTesting, OneReadinessEventServesEveryWaitingAccept)
{
    const auto address = TestSocketAddress();
    auto acceptor = Foundation::NBIO::bind(address);
    ASSERT_NE(acceptor, nullptr);

    // The clients connect before anybody accepts, so one readiness event later all
    // of them are there to be taken.
    auto clients = connect_clients(address, kWriters);

    std::size_t accepted = 0;
    Foundation::NBIO::run(accept_all(*acceptor, accepted));

    EXPECT_EQ(accepted, kWriters) << "every waiter has to be answered";
}

// The completion backend takes connections one at a time instead of draining, and
// the queue is what keeps every waiter in line behind them.
TEST(TcpSocketChannelTesting, EveryWaitingAcceptIsAnsweredOnURing)
{
    Foundation::NBIO::initialize(std::make_unique<Foundation::NBIO::URingMultiplexer>());

    const auto address = TestSocketAddress();
    auto acceptor = Foundation::NBIO::bind(address);
    ASSERT_NE(acceptor, nullptr);

    auto clients = connect_clients(address, kWriters);

    std::size_t accepted = 0;
    Foundation::NBIO::run(accept_all(*acceptor, accepted));

    EXPECT_EQ(accepted, kWriters) << "every waiter has to be answered";
}

TEST(TcpSocketChannelTesting, SendsFromManyCoroutinesKeepTheOrderTheyQueuedIn)
{
    const auto address = TestSocketAddress();
    auto acceptor = Foundation::NBIO::bind(address);
    ASSERT_NE(acceptor, nullptr);

    auto client = Foundation::Core::TcpSocket{address.family(), Foundation::Core::TcpSocket::Type::kStream};
    ASSERT_TRUE(client.connect(address));

    Foundation::NBIO::run(accept_and_send(*acceptor));

    // Read the whole stream back: everything every writer asked to send, and in an
    // order that keeps each writer's own chunks in the order it wrote them.
    std::size_t expected = 0;
    for (std::size_t writer = 0; writer < kWriters; ++writer)
    {
        for (std::size_t index = 0; index < kChunks; ++index)
        {
            expected += ChunkBytes(writer, index).size();
        }
    }

    std::string received;
    std::array<char, 4096> buffer{};
    while (received.size() < expected)
    {
        const auto got = client.receive(std::span<char>{buffer.data(), buffer.size()});
        ASSERT_TRUE(got) << "the client could not read the stream";
        ASSERT_GT(*got, 0U);
        received.append(buffer.data(), *got);
    }
    ASSERT_EQ(received.size(), expected);

    std::size_t offset = 0;
    std::vector<std::size_t> next_index(kWriters, 0);
    while (offset < received.size())
    {
        const auto end = received.find(';', offset);
        ASSERT_NE(end, std::string::npos) << "a chunk was cut short at " << offset;
        const std::string token = received.substr(offset, end - offset);
        const auto separator = token.find(':');
        ASSERT_NE(separator, std::string::npos) << token;
        const std::size_t writer = std::stoul(token.substr(6, separator - 6));
        const std::size_t index = std::stoul(token.substr(separator + 1));
        ASSERT_LT(writer, kWriters) << token;
        EXPECT_EQ(index, next_index[writer]) << "writer " << writer << " lost its order";
        next_index[writer] = index + 1;
        offset = end + 1;
    }

    for (std::size_t writer = 0; writer < kWriters; ++writer)
    {
        EXPECT_EQ(next_index[writer], kChunks) << "writer " << writer << " lost a chunk";
    }
}

TEST(TcpSocketChannelTesting, ReceivesFromManyCoroutinesShareWhatArrived)
{
    const auto address = TestSocketAddress();
    auto acceptor = Foundation::NBIO::bind(address);
    ASSERT_NE(acceptor, nullptr);

    auto client = Foundation::Core::TcpSocket{address.family(), Foundation::Core::TcpSocket::Type::kStream};
    ASSERT_TRUE(client.connect(address));

    constexpr std::size_t kReceivers = 4;
    constexpr std::size_t kBytesPerReceiver = kChunkBytes;
    constexpr std::size_t kBytes = kReceivers * kBytesPerReceiver;

    const std::string sent(kBytes, 'x');
    std::vector<std::string> received(kReceivers);

    Foundation::NBIO::run(accept_and_receive(*acceptor, client, sent, received, kBytesPerReceiver));

    // Whatever arrived was handed to the waiters in the order they queued, so each
    // one of them has some of it and together they have all of it.
    std::size_t total = 0;
    for (std::size_t index = 0; index < kReceivers; ++index)
    {
        EXPECT_GT(received[index].size(), 0U) << "receiver " << index << " was never answered";
        total += received[index].size();
    }
    EXPECT_EQ(total, kBytes) << "every byte that arrived belongs to one of the waiters";
}
