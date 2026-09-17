// Echo over RDMA: the server reflects whatever it receives, the client sends a
// line and prints the reply.
//
//   RDMA_Echo server 0.0.0.0 7471
//   RDMA_Echo client 10.0.0.1 7471 "hello"
//
// Requires an RDMA device (a real HCA, or soft-RoCE: `rdma link add rxe0 type
// rxe netdev eth0`).
#if defined(__linux__)

#include <Foundation/Core/Address.hpp>
#include <Foundation/Core/RDMA_Acceptor.hpp>
#include <Foundation/Core/RDMA_Connector.hpp>

#include <cstdio>
#include <cstring>
#include <algorithm>
#include <span>
#include <string>
#include <string_view>

namespace Core = Foundation::Core;

namespace
{
// Must match on both sides: a message has to fit one posted receive chunk.
constexpr std::size_t kChunkSize = 4096;
constexpr std::size_t kChunks = 1024;

// Drains every chunk the peer has filled, handing each back so it is posted
// again for the next message.
std::string take(Core::RDMA_Stream &stream)
{
    std::string data;
    while (auto chunk = stream.receive())
    {
        data.append(chunk->data(), chunk->size());
        stream.release(*chunk);
    }
    return data;
}

// Posts as much of `payload` as it can, keeping several sends in flight, and
// waits only when the send window is full. Returns the bytes posted.
std::size_t pump(Core::RDMA_Stream &stream, std::string_view payload, std::size_t offset)
{
    while (offset < payload.size() && !stream.peer_closed())
    {
        auto chunk = stream.acquire();
        if (!chunk)
        {
            stream.poll(-1); // the send window is full
            continue;
        }
        const auto length = std::min(chunk->size(), payload.size() - offset);
        std::memcpy(chunk->data(), payload.data() + offset, length);
        if (const auto error = stream.send(*chunk, length); error)
        {
            break;
        }
        offset += length;
    }
    return offset;
}

int run_server(const char *host, std::uint16_t port)
{
    Core::RDMA_Acceptor acceptor(Core::BitmapMemory(kChunkSize, kChunks), Core::BitmapMemory(kChunkSize, kChunks));
    acceptor.listen(Core::Address::from_ipv4(host, port));
    std::printf("listening on %s:%u\n", host, port);

    // An accept that comes back empty is not a failure: it means the event was
    // about a connection that is already established, which this channel also
    // reports. Wait for the next one.
    auto accepted = acceptor.accept();
    while (!accepted)
    {
        accepted = acceptor.accept();
    }
    Core::RDMA_Stream stream = std::move(*accepted);
    std::printf("accepted\n");

    std::size_t echoed = 0;
    std::size_t offset = 0;
    std::string backlog;
    while (!stream.peer_closed())
    {
        stream.poll(-1);
        backlog += take(stream);
        if (!backlog.empty())
        {
            offset = pump(stream, backlog, offset);
            echoed += offset;
            backlog.erase(0, offset);
            offset = 0;
        }
    }
    std::printf("echoed %zu bytes, peer closed: %s\n", echoed, stream.error_message());
    return 0;
}

int run_client(const char *host, std::uint16_t port, std::string_view payload)
{
    Core::RDMA_Connector connector(Core::BitmapMemory(kChunkSize, kChunks), Core::BitmapMemory(kChunkSize, kChunks));
    auto stream = connector.connect(Core::Address::from_ipv4(host, port));
    std::printf("connected\n");

    std::size_t offset = 0;
    std::string reply;
    while (reply.size() < payload.size() && !stream.peer_closed())
    {
        offset = pump(stream, payload, offset);
        stream.poll(-1);
        reply += take(stream);
    }
    std::printf("sent %zu, reply %zu bytes, status: %s\n", offset, reply.size(), stream.error_message());
    std::printf("received %zu bytes before exit\n", reply.size());
    return reply == payload ? 0 : 1;
}
} // namespace

int main(int argc, char **argv)
{
    // Unbuffered, otherwise a killed run loses its diagnostics.
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if (argc < 4)
    {
        std::fprintf(stderr, "usage: %s server|client <ip> <port> [payload]\n", argv[0]);
        return 2;
    }

    const std::string_view mode{argv[1]};
    const auto port = static_cast<std::uint16_t>(std::atoi(argv[3]));

    try
    {
        if (mode == "server")
        {
            return run_server(argv[2], port);
        }
        if (mode == "client")
        {
            std::string payload{argc > 4 ? argv[4] : "hello rdma"};
            // "@N" generates N bytes, which argv cannot carry for large sizes.
            if (payload.size() > 1 && payload.front() == '@')
            {
                payload.assign(std::stoul(payload.substr(1)), 'x');
            }
            return run_client(argv[2], port, payload);
        }
    }
    catch (const std::exception &error)
    {
        std::fprintf(stderr, "error: %s\n", error.what());
        return 1;
    }

    std::fprintf(stderr, "unknown mode: %.*s\n", static_cast<int>(mode.size()), mode.data());
    return 2;
}

#else
int main()
{
    return 0;
}
#endif // defined(__linux__)
