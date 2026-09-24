// Awaitable echo over RDMA: exercises the NBIO RDMA channels end to end.
//
//   RdmaAsyncEcho server 192.168.0.201 1234
//   RdmaAsyncEcho client 192.168.0.201 1234 32768
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
#include <Foundation/NBIO/RdmaAcceptChannel.hpp>
#include <Foundation/NBIO/RdmaConnectChannel.hpp>
#include <Foundation/NBIO/RdmaSession.hpp>
#include <Foundation/NBIO/URingMultiplexer.hpp>

#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/RdmaAcceptor.hpp>
#include <Foundation/Core/RdmaConnector.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <optional>
#include <span>
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
    Core::RdmaAcceptor acceptor(make_pool(), make_pool());
    if (const auto listening = acceptor.listen(Core::SocketAddress::from_v4(host, port)); !listening) [[unlikely]]
    {
        std::fprintf(stderr, "listen failed: %s\n", listening.error().c_str());
        co_return;
    }
    std::printf("listening on %s:%u\n", host, port);

    NBIO::RdmaAcceptChannel accept_channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
    auto accepted = co_await accept_channel.accept();
    if (!accepted) [[unlikely]]
    {
        spdlog::warn("accept failed: {}", accepted.error());
        co_return;
    }
    std::shared_ptr<NBIO::RdmaSession> session = std::move(*accepted);
    std::printf("accepted\n");

    std::size_t echoed = 0;
    while (true)
    {
        auto received = co_await session->receive();
        if (!received) [[unlikely]]
        {
            spdlog::warn("receive failed: {}", received.error());
            break;
        }
        std::optional<std::span<char>> incoming = std::move(*received);
        if (!incoming)
        {
            // No chunk and no error means the peer closed the connection.
            break;
        }

        // One send in flight at a time: the echo has to stay in order, and a
        // chunk only comes back once its completion has been reaped.
        auto acquired = session->send_channel().acquire();
        if (!acquired) [[unlikely]]
        {
            spdlog::warn("the send channel failed: {}", acquired.error());
            (void)session->release(*incoming);
            break;
        }
        if (!*acquired) [[unlikely]]
        {
            spdlog::warn("no free send chunk");
            (void)session->release(*incoming);
            break;
        }

        const auto outgoing = **acquired;
        const auto length = std::min(outgoing.size(), incoming->size());
        std::memcpy(outgoing.data(), incoming->data(), length);

        const auto posted = session->send(outgoing, length);
        const auto released = session->release(*incoming);
        if (!posted) [[unlikely]]
        {
            spdlog::warn("send failed: {}", posted.error());
            break;
        }
        if (!released) [[unlikely]]
        {
            spdlog::warn("release failed: {}", released.error());
            break;
        }
        const auto reaped = co_await session->poll_send(0);
        if (!reaped) [[unlikely]]
        {
            spdlog::warn("the link stopped reporting completions: {}", reaped.error());
            break;
        }
        echoed += length;
    }

    std::printf("echoed %zu bytes, status: %s\n", echoed, session->stream().error().c_str());
}

// Sends the payload one chunk at a time and consumes the echo of each message
// before sending the next, so the receive window can never fill up.
NBIO::Task<void> RunClient(const char *host, std::uint16_t port, std::size_t payload_size, bool &matched)
{
    Core::RdmaConnector connector(make_pool(), make_pool());
    NBIO::RdmaConnectChannel connect_channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());

    auto connected = co_await connect_channel.connect(Core::SocketAddress::from_v4(host, port));
    if (!connected) [[unlikely]]
    {
        spdlog::warn("connect failed: {}", connected.error());
        matched = false;
        co_return;
    }
    std::shared_ptr<NBIO::RdmaSession> session = std::move(*connected);
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
        auto acquired = session->send_channel().acquire();
        if (!acquired) [[unlikely]]
        {
            spdlog::warn("the send channel failed after {} bytes: {}", sent, acquired.error());
            ok = false;
            break;
        }
        if (!*acquired) [[unlikely]]
        {
            spdlog::warn("no free send chunk after {} bytes", sent);
            ok = false;
            break;
        }

        const auto outgoing = **acquired;
        const auto length = std::min(outgoing.size(), payload.size() - sent);
        std::memcpy(outgoing.data(), payload.data() + sent, length);

        if (const auto posted = session->send(outgoing, length); !posted)
        {
            spdlog::warn("send failed: {}", posted.error());
            ok = false;
            break;
        }
        // Waited for here, not because the reply needs it, but because the
        // chunk has to come back before the next message can be built.
        if (const auto reaped = co_await session->poll_send(0); !reaped) [[unlikely]]
        {
            spdlog::warn("the link stopped reporting completions: {}", reaped.error());
            ok = false;
            break;
        }
        sent += length;

        auto got = co_await session->receive();
        if (!got) [[unlikely]]
        {
            spdlog::warn("receive failed after {} bytes: {}", received, got.error());
            ok = false;
            break;
        }
        std::optional<std::span<char>> incoming = std::move(*got);
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
        if (const auto released = session->release(*incoming); !released) [[unlikely]]
        {
            spdlog::warn("release failed: {}", released.error());
            ok = false;
            break;
        }
    }

    matched = ok && sent == payload.size() && received == payload.size();
    std::printf("sent %zu, received %zu, status: %s\n", sent, received, session->stream().error().c_str());
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
