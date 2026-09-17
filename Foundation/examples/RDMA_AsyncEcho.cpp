// Awaitable echo over RDMA: exercises the NBIO RDMA channels end to end.
//
//   RDMA_AsyncEcho server 192.168.0.201 1234
//   RDMA_AsyncEcho client 192.168.0.201 1234 32768
//
// The server accepts one connection and reflects every message it receives. The
// client sends its payload one chunk per message and checks each echo against a
// position-dependent pattern, so both directions and their ordering are driven
// through the coroutine API rather than a poll loop.
//
// Either multiplexer can drive the same channels: set NBIO_BACKEND=uring to run
// on io_uring instead of the default epoll backend.
//
// Requires an RDMA device (a real HCA, or soft-iWARP:
// `rdma link add siw0 type siw netdev eth0`).
#if defined(__linux__)

#include <Foundation/NBIO/NBIO.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/RDMA_AcceptChannel.hpp>
#include <Foundation/NBIO/RDMA_ConnectChannel.hpp>
#include <Foundation/NBIO/RDMA_Session.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <Foundation/Core/Address.hpp>
#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/RDMA_Acceptor.hpp>
#include <Foundation/Core/RDMA_Connector.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <string_view>
#include <vector>

namespace Core = Foundation::Core;
namespace NBIO = Foundation::NBIO;

namespace
{
constexpr std::size_t kChunkSize = 4096;
constexpr std::size_t kChunks = 64;

Core::BitmapMemory make_pool()
{
    return Core::BitmapMemory(kChunkSize, kChunks);
}

// Accepts one connection, then echoes every message until the peer goes away.
// The acceptor is declared before the channels and the session so that the
// stream, which borrows the protection domain and the pools, is torn down first.
NBIO::Task<void> RunServer(const char *host, std::uint16_t port)
{
    Core::RDMA_Acceptor acceptor(make_pool(), make_pool());
    acceptor.listen(Core::Address::from_ipv4(host, port));
    std::printf("listening on %s:%u\n", host, port);

    NBIO::RDMA_AcceptChannel accept_channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
    auto session = co_await accept_channel.accept();
    std::printf("accepted\n");

    std::size_t echoed = 0;
    while (true)
    {
        auto incoming = co_await session->receive();
        if (!incoming)
        {
            // No chunk and no error means the peer closed the connection.
            break;
        }

        // One send in flight at a time: the echo has to stay in order, and a
        // chunk only comes back once its completion has been reaped.
        auto outgoing = session->send_channel().acquire();
        if (!outgoing) [[unlikely]]
        {
            spdlog::warn("no free send chunk");
            session->release(*incoming);
            break;
        }

        const auto length = std::min(outgoing->size(), incoming->size());
        std::memcpy(outgoing->data(), incoming->data(), length);

        const auto error = session->send(*outgoing, length);
        session->release(*incoming);
        if (error)
        {
            spdlog::warn("send failed: {}", error.message());
            break;
        }
        (void)co_await session->poll_send(0);
        echoed += length;
    }

    std::printf("echoed %zu bytes, status: %s\n", echoed, session->stream().error_message());
}

// Sends the payload one chunk at a time and consumes the echo of each message
// before sending the next, so the receive window can never fill up.
NBIO::Task<void> RunClient(const char *host, std::uint16_t port, std::size_t payload_size, bool &matched)
{
    Core::RDMA_Connector connector(make_pool(), make_pool());
    NBIO::RDMA_ConnectChannel connect_channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());

    auto session = co_await connect_channel.connect(Core::Address::from_ipv4(host, port));
    std::printf("connected\n");

    // A position-dependent pattern, so a reply that arrives out of order or
    // short shows up as a mismatch rather than as a plausible byte count.
    std::vector<char> payload(payload_size);
    for (std::size_t i = 0; i < payload.size(); ++i)
    {
        payload[i] = static_cast<char>('a' + (i % 26));
    }

    std::size_t sent = 0;
    std::size_t received = 0;
    bool ok = true;

    while (sent < payload.size() && ok)
    {
        auto outgoing = session->send_channel().acquire();
        if (!outgoing) [[unlikely]]
        {
            spdlog::warn("no free send chunk after {} bytes", sent);
            ok = false;
            break;
        }

        const auto length = std::min(outgoing->size(), payload.size() - sent);
        std::memcpy(outgoing->data(), payload.data() + sent, length);

        if (const auto error = session->send(*outgoing, length); error)
        {
            spdlog::warn("send failed: {}", error.message());
            ok = false;
            break;
        }
        // Waited for here, not because the reply needs it, but because the
        // chunk has to come back before the next message can be built.
        (void)co_await session->poll_send(0);
        sent += length;

        auto incoming = co_await session->receive();
        if (!incoming)
        {
            std::printf("peer closed after %zu bytes echoed\n", received);
            ok = false;
            break;
        }

        const bool in_range = received + incoming->size() <= payload.size();
        if (!in_range || !std::equal(incoming->begin(), incoming->end(), payload.begin() + received))
        {
            std::printf("echo mismatch at offset %zu (%zu bytes)\n", received, incoming->size());
            ok = false;
        }
        received += incoming->size();
        session->release(*incoming);
    }

    matched = ok && sent == payload.size() && received == payload.size();
    std::printf("sent %zu, received %zu, status: %s\n", sent, received, session->stream().error_message());
}
} // namespace

int main(int argc, char **argv)
{
    // Unbuffered, otherwise a killed run loses its diagnostics.
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // Install the backend before anything touches the engine. The RDMA channels
    // are backend-agnostic: they only need their fd watched for readability.
    if (const char *backend = std::getenv("NBIO_BACKEND");
        backend != nullptr && std::string_view{backend} == "uring")
    {
        NBIO::initialize(std::make_unique<NBIO::URingMultiplexer>());
    }

    if (argc < 4)
    {
        std::fprintf(stderr, "usage: %s server|client <ip> <port> [bytes]\n", argv[0]);
        return 2;
    }

    const std::string_view mode{argv[1]};
    const auto port = static_cast<std::uint16_t>(std::atoi(argv[3]));

    try
    {
        if (mode == "server")
        {
            NBIO::run(RunServer(argv[2], port));
            return 0;
        }
        if (mode == "client")
        {
            const auto payload_size = argc > 4 ? static_cast<std::size_t>(std::stoul(argv[4])) : kChunkSize;
            bool matched = false;
            NBIO::run(RunClient(argv[2], port, payload_size, matched));
            return matched ? 0 : 1;
        }
    }
    catch (const std::exception &error)
    {
        std::fprintf(stderr, "error: %s\n", error.what());
        return 2;
    }

    std::fprintf(stderr, "unknown mode: %.*s\n", static_cast<int>(mode.size()), mode.data());
    return 2;
}

#endif // defined(__linux__)
