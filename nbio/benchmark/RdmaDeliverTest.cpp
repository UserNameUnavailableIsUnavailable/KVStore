// The delivery layer over an rdma session, exercised end to end: both ends of one
// connection in this process, a thread and an engine each, an RdmaDeliverService over
// an RdmaSessionService apiece. The sender pushes a payload, the receiver checks every
// byte of it and answers with what it counted, and the sender checks the answer.
//
// What it is here to catch is the protocol rather than the device: a sequence that does
// not line up, a window that lets a sender outrun the receives the peer has posted, an
// acknowledgement that goes out before the buffer is actually back, a payload that
// arrives out of order, or a shutdown that leaves a coroutine Parked so that a thread's
// Run() never returns.
#if defined(__linux__)

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <CLI/CLI.hpp>
#include <nbio/utility/Bitmap.hpp>
#include <nbio/utility/Byte.hpp>
#include <nbio/utility/Expected.hpp>
#include <nbio/net/RdmaAcceptor.hpp>
#include <nbio/net/RdmaConnector.hpp>
#include <nbio/net/RdmaHeader.hpp>
#include <nbio/net/RdmaResourceManager.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/core/EpollMultiplexer.hpp>
#include <nbio/nbio.hpp>
#include <nbio/net/RdmaAcceptChannel.hpp>
#include <nbio/net/RdmaConnectChannel.hpp>
#include <nbio/net/RdmaDeliverService.hpp>
#include <nbio/core/URingMultiplexer.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {



using Clock = std::chrono::steady_clock;

// A megabyte is enough to be cut into more packets than the window holds, which is what
// makes the sender wait for an acknowledgement rather than posting everything at once.
constexpr std::size_t kDefaultBytes = 1U << 20;

// What the receiver answers with: the count and the sum of what it checked.
struct Report {
    std::uint64_t bytes{0};
    std::uint64_t checksum{0};
};

constexpr std::size_t kReportBytes = 2 * sizeof(std::uint64_t);

// What one end of the exchange ended up with. `ok` is false for one that did not finish,
// and `failure` then says why -- the one thing either end reports.
struct Outcome {
    bool ok{false};
    std::size_t bytes{0};
    double seconds{0.0};
    std::uint64_t sent{0};
    std::uint64_t acknowledged{0};
    std::uint64_t taken{0};
    std::string failure;
};

// How far the exchange has got, for the watcher: the first question about one that never
// finishes is which end stopped.
struct Progress {
    std::atomic<std::size_t> moved{0};
};

// The payload's bytes are a function of their offset, so the receiver can check any
// packet against where it belongs rather than against what it expected next -- which is
// what makes a packet that arrives out of order a failure that says so.
char PatternByte(std::size_t offset) noexcept { return static_cast<char>((offset * 131 + 7) & 0xFF); }

std::uint64_t PatternSum(std::size_t bytes) noexcept {
    std::uint64_t sum = 0;
    for (std::size_t offset = 0; offset < bytes; ++offset) {
        sum += static_cast<unsigned char>(PatternByte(offset));
    }
    return sum;
}

std::array<char, kReportBytes> EncodeReport(const Report& report) noexcept {
    std::array<char, kReportBytes> bytes{};
    const auto count = nbio::utility::ToBigEndian(report.bytes);
    const auto sum = nbio::utility::ToBigEndian(report.checksum);
    std::memcpy(bytes.data(), &count, sizeof(count));
    std::memcpy(bytes.data() + sizeof(count), &sum, sizeof(sum));
    return bytes;
}

bool DecodeReport(std::span<const char> bytes, Report& report) noexcept {
    if (bytes.size() != kReportBytes) {
        return false;
    }
    std::uint64_t count = 0;
    std::uint64_t sum = 0;
    std::memcpy(&count, bytes.data(), sizeof(count));
    std::memcpy(&sum, bytes.data() + sizeof(count), sizeof(sum));
    report.bytes = nbio::utility::FromBigEndian(count);
    report.checksum = nbio::utility::FromBigEndian(sum);
    return true;
}

double SecondsSince(Clock::time_point started) noexcept {
    return std::chrono::duration<double>(Clock::now() - started).count();
}

// Takes `bytes` of the pattern, checking each one against its own offset, and gives every
// packet's chunk back as it goes -- which is also what moves the sender's window along,
// so a receiver that holds payloads holds the sender with them.
nbio::Task<nbio::utility::expected<void, std::string>> Take(nbio::net::RdmaDeliverService& service, std::size_t bytes,
                                                   Progress& progress) {
    std::size_t received = 0;
    while (received < bytes) {
        auto incoming = co_await service.Receive();
        if (!incoming) [[unlikely]] {
            co_return nbio::utility::unexpected(incoming.error());
        }
        if (!*incoming) [[unlikely]] {
            // The one thing that ends a receive with nothing in it is the link being over.
            co_return nbio::utility::unexpected("the link ended after " + std::to_string(received) + " of " +
                                       std::to_string(bytes) + " bytes");
        }

        const std::span<char> payload = **incoming;
        if (payload.size() > bytes - received) [[unlikely]] {
            co_return nbio::utility::unexpected("a packet carried " + std::to_string(payload.size()) + " bytes where " +
                                       std::to_string(bytes - received) + " were still expected");
        }
        for (std::size_t offset = 0; offset < payload.size(); ++offset) {
            if (payload[offset] != PatternByte(received + offset)) [[unlikely]] {
                co_return nbio::utility::unexpected("byte " + std::to_string(received + offset) + " arrived as " +
                                           std::to_string(static_cast<unsigned char>(payload[offset])));
            }
        }
        received += payload.size();
        progress.moved.store(received, std::memory_order_relaxed);

        if (auto released = co_await service.release(payload); !released) [[unlikely]]
        {
            co_return nbio::utility::unexpected(released.error());
        }
    }
    co_return nbio::utility::expected<void, std::string>{};
}

// The sending end: wait to be admitted, say what this end can take, push the payload,
// then wait for the receiver's report. The report is what says the payload arrived whole
// and in order, so the clock is read when it lands rather than when the last packet was
// posted -- a posted send is not a sent one.
nbio::Task<void> SendEnd(nbio::net::RdmaAcceptChannel& channel, nbio::net::RdmaDeliverService::Layout layout, std::size_t bytes,
                         Outcome& outcome, Progress& progress) {
    auto admitted = co_await channel.accept();
    if (!admitted) [[unlikely]] {
        outcome.failure = "the connection was not admitted: " + admitted.error();
        co_return;
    }

    auto service = std::make_shared<nbio::net::RdmaDeliverService>(*admitted, layout);
    if (auto ready = co_await service->handshake(); !ready) [[unlikely]]
    {
        outcome.failure = "the handshake failed: " + ready.error();
        co_return;
    }
    // Only now: nothing is taken off the session until the reader is running, and the
    // handshake is the one packet that is read without it.
    service->Start();

    std::vector<char> payload(bytes);
    for (std::size_t offset = 0; offset < payload.size(); ++offset) {
        payload[offset] = PatternByte(offset);
    }

    const auto started = Clock::now();
    if (auto posted = co_await service->Send(payload); !posted) [[unlikely]]
    {
        outcome.failure = "the payload could not be sent: " + posted.error();
        co_return;
    }

    auto answer = co_await service->Receive();
    if (!answer) [[unlikely]] {
        outcome.failure = "the answer could not be received: " + answer.error();
        co_return;
    }
    if (!*answer) [[unlikely]] {
        outcome.failure = "the link ended before the receiver answered";
        co_return;
    }
    Report report;
    if (!DecodeReport(**answer, report)) [[unlikely]] {
        outcome.failure = "the answer was not a report";
        co_return;
    }
    (void)co_await service->release(**answer);

    outcome.seconds = SecondsSince(started);
    if (report.bytes != bytes) [[unlikely]] {
        outcome.failure =
            "the receiver counted " + std::to_string(report.bytes) + " of " + std::to_string(bytes) + " bytes";
        co_return;
    }
    if (report.checksum != PatternSum(bytes)) [[unlikely]] {
        outcome.failure = "the receiver summed the payload to " + std::to_string(report.checksum) + ", not " +
                          std::to_string(PatternSum(bytes));
        co_return;
    }

    outcome.ok = true;
    outcome.bytes = bytes;
    outcome.sent = service->sent();
    outcome.acknowledged = service->acknowledged();
    outcome.taken = service->taken();
    service->Stop();
    co_return;
}

// The receiving end, and the one the window is really about: it takes the payload,
// releases each packet, and answers. Its own reader is what advances the sender's window,
// so an acknowledgement that went out at the wrong moment would stall the sender here.
nbio::Task<void> ReceiveEnd(nbio::net::RdmaConnectChannel& channel, nbio::net::Address master,
                            nbio::net::RdmaDeliverService::Layout layout, std::size_t bytes, Outcome& outcome,
                            Progress& progress) {
    auto connected = co_await channel.Connect(master);
    if (!connected) [[unlikely]] {
        outcome.failure = "the connection was not established: " + connected.error();
        co_return;
    }

    auto service = std::make_shared<nbio::net::RdmaDeliverService>(*connected, layout);
    if (auto ready = co_await service->handshake(); !ready) [[unlikely]]
    {
        outcome.failure = "the handshake failed: " + ready.error();
        co_return;
    }
    service->Start();

    const auto started = Clock::now();
    if (auto taken = co_await Take(*service, bytes, progress); !taken) [[unlikely]]
    {
        outcome.failure = taken.error();
        co_return;
    }
    outcome.seconds = SecondsSince(started);

    const auto report = EncodeReport(Report{.bytes = bytes, .checksum = PatternSum(bytes)});
    if (auto posted = co_await service->Send(report); !posted) [[unlikely]]
    {
        outcome.failure = "the answer could not be sent: " + posted.error();
        co_return;
    }

    outcome.ok = true;
    outcome.bytes = bytes;
    outcome.sent = service->sent();
    outcome.acknowledged = service->acknowledged();
    outcome.taken = service->taken();
    // The answer is posted, so the device will deliver it whether or not the reader is
    // still there to be told about anything -- which is what lets both ends stop as soon
    // as they have finished rather than agreeing on who goes first.
    service->Stop();
    co_return;
}

void Print(const Outcome& outcome, const char* end) {
    if (!outcome.ok) [[unlikely]] {
        std::printf("%-8s FAILED: %s\n", end, outcome.failure.c_str());
    } else {
        const auto mib = static_cast<double>(outcome.bytes) / (1024.0 * 1024.0) / outcome.seconds;
        std::printf(
            "%-8s %zu bytes in %.3f s = %.1f MiB/s (report included), %llu packets sent, %llu acknowledged, "
            "%llu taken\n",
            end, outcome.bytes, outcome.seconds, mib, static_cast<unsigned long long>(outcome.sent),
            static_cast<unsigned long long>(outcome.acknowledged), static_cast<unsigned long long>(outcome.taken));
    }
    // Line by line, as it happens: an end that has finished says so while the other one
    // may still be running, and a report held in a buffer is one nobody sees when the
    // process is stopped.
    std::fflush(stdout);
}

// A transfer that stalls says nothing by itself, and a test that hangs with no output is
// one nobody can tell apart from a slow one. The watcher turns the hang into a report --
// how far the exchange got, which is the first thing anyone asks -- and then ends the
// process so the next run starts from a number rather than from a hang.
void Watch(Progress& progress, std::chrono::seconds budget, std::mutex& done_mutex, std::condition_variable& done,
           bool& finished, std::size_t bytes) {
    std::unique_lock lock(done_mutex);
    if (done.wait_for(lock, budget, [&finished] { return finished; })) {
        return;
    }
    std::printf("stalled after %lld s: %zu of %zu bytes moved\n", static_cast<long long>(budget.count()),
                progress.moved.load(std::memory_order_relaxed), bytes);
    std::fflush(stdout);
    ::_exit(1);
}

// A port to listen on that nothing else has. An rdma listener cannot ask the kernel for
// one the way a TCP listener can -- rdma_bind_addr with port 0 leaves the id on a port
// nobody can name -- so the free port is asked of the TCP stack and then used for the
// rdma listener.
std::uint16_t FreePort() {
    const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe < 0) {
        return 0;
    }

    ::sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    std::uint16_t port = 0;
    if (::bind(probe, reinterpret_cast<::sockaddr*>(&address), sizeof(address)) == 0) {
        ::socklen_t length = sizeof(address);
        if (::getsockname(probe, reinterpret_cast<::sockaddr*>(&address), &length) == 0) {
            port = ::ntohs(address.sin_port);
        }
    }
    ::close(probe);
    return port;
}

std::unique_ptr<nbio::core::Multiplexer> MakeMultiplexer(const std::string& name) {
    if (name == "epoll") {
        return std::make_unique<nbio::core::EpollMultiplexer>();
    }
    if (name == "io_uring") {
        return std::make_unique<nbio::core::URingMultiplexer>();
    }
    throw std::invalid_argument("--multiplexer must be 'epoll' or 'io_uring'");
}

// Installs an engine on this thread and drives one end of the exchange on it. The only
// thing that can throw here is what is built before the exchange starts, and a run has
// one place its outcome is written, so it is written from here too.
template <typename Body>
void RunEngine(Body body, const std::string& multiplexer, Outcome& outcome) {
    try {
        nbio::initialize(MakeMultiplexer(multiplexer));
        body();
    } catch (const std::exception& error) {
        outcome.failure = error.what();
    } catch (...) {
        outcome.failure = "unknown failure";
    }
}
}  // namespace

int main(int argc, char* argv[]) {
    CLI::App application{"RdmaDeliverService: a payload each way over one connection"};

    std::string address;
    std::string device;
    std::uint16_t port = 0;
    std::size_t bytes = kDefaultBytes;
    std::string multiplexer = "epoll";

    application.add_option("--address", address, "address of the rdma device to run on")
        ->envname("KVSTORE_RDMA_ADDRESS")
        ->required();
    application.add_option("--rdma-device", device, "rdma device, by name (--rdma-device siw0)")
        ->envname("KVSTORE_RDMA_DEVICE")
        ->required();
    application.add_option("--port", port, "port to listen on; 0 picks a free one")->check(CLI::Range(0, 65535));
    application.add_option("--bytes", bytes, "payload to move, in bytes (default 1 MiB)");
    application.add_option("--multiplexer", multiplexer, "I/O multiplexer: epoll or io_uring")
        ->check(CLI::IsMember({"epoll", "io_uring"}));

    try {
        application.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return application.exit(error);
    }

    // Two managers, one per engine: the manager is single-threaded, and each end of a
    // link is driven by an engine of its own.
    nbio::net::RdmaResourceManager sending_resources(device);
    nbio::net::RdmaResourceManager receiving_resources(device);

    const auto send_chunk = sending_resources.send_memory().chunk_size();
    const auto receive_chunk = receiving_resources.receive_memory().chunk_size();
    const auto pool = receiving_resources.receive_memory().capacity();
    if (send_chunk == 0 || receive_chunk == 0 || pool == 0) [[unlikely]] {
        std::printf("the manager's chunks hold nothing\n");
        return 1;
    }
    if (send_chunk != receive_chunk) [[unlikely]] {
        // A send chunk smaller than a receive chunk is fine -- the sender cuts at the
        // smaller of the two -- but it would mean this test is not measuring the layout
        // it thinks it is.
        std::printf("the send chunks are %zu bytes and the receive chunks %zu\n", send_chunk, receive_chunk);
        return 1;
    }

    // What this end can take, as the handshake says it. The count is how many receives the
    // connection has posted -- not the receive pool's size, which is thousands of chunks
    // and says nothing about what the device has actually been handed. Advertise the pool
    // and the peer will fill the window and then fail on it with RNR.
    const nbio::net::RdmaDeliverService::Layout layout{.chunk_size = receive_chunk,
                                                  .chunk_count = nbio::net::RdmaConnector::kReceiveChunks};
    const auto packet_payload = layout.payload_size();
    if (packet_payload == 0) [[unlikely]] {
        std::printf("a chunk of %zu bytes holds no payload once the header is in it\n", receive_chunk);
        return 1;
    }

    const auto listening_on = port != 0 ? port : FreePort();
    if (listening_on == 0) [[unlikely]] {
        std::printf("no free port to listen on\n");
        return 1;
    }

    nbio::net::RdmaAcceptor acceptor(sending_resources);
    const auto listening = acceptor.listen(nbio::net::Address::FromV4(address, listening_on));
    if (!listening) [[unlikely]] {
        std::printf("cannot listen on %s:%u: %s\n", address.c_str(), listening_on, listening.error().c_str());
        return 1;
    }
    const nbio::net::Address master = nbio::net::Address::FromV4(address, listening_on);

    std::printf("%zu bytes as %zu packets of %zu bytes, window of %zu of a pool of %zu, %s:%u on %s, %s\n", bytes,
                (bytes + packet_payload - 1) / packet_payload, packet_payload, layout.chunk_count, pool,
                address.c_str(), listening_on, device.c_str(), multiplexer.c_str());
    std::fflush(stdout);

    Outcome sending;
    Outcome receiving;
    Progress progress;
    std::mutex done_mutex;
    std::condition_variable done;
    bool finished = false;
    // One second per megabyte, and five seconds over that: enough for a software device
    // on a slow day, and far less than forever.
    const auto budget = std::chrono::seconds(5 + static_cast<std::int64_t>(bytes / (1U << 20)));
    std::thread watcher(Watch, std::ref(progress), budget, std::ref(done_mutex), std::ref(done), std::ref(finished),
                        bytes);

    std::thread sender([&] {
        RunEngine(
            [&] {
                nbio::net::RdmaAcceptChannel channel(acceptor, nbio::runtime::multiplexer(), nbio::runtime::scheduler());
                nbio::run(SendEnd(channel, layout, bytes, sending, progress));
            },
            multiplexer, sending);
        Print(sending, "sender");
    });
    std::thread receiver([&] {
        RunEngine(
            [&] {
                nbio::net::RdmaConnector connector(receiving_resources);
                nbio::net::RdmaConnectChannel channel(connector, nbio::runtime::multiplexer(), nbio::runtime::scheduler());
                nbio::run(ReceiveEnd(channel, master, layout, bytes, receiving, progress));
            },
            multiplexer, receiving);
        Print(receiving, "receiver");
    });

    // Both ends are joined before the acceptor and the managers go: every session
    // borrows them, and the sessions live on the threads.
    sender.join();
    receiver.join();
    {
        const std::lock_guard lock(done_mutex);
        finished = true;
    }
    done.notify_all();
    watcher.join();

    if (!sending.ok || !receiving.ok) {
        return 1;
    }
    std::printf("ok: %zu bytes each way, %zu packets of %zu bytes over a window of %zu\n", bytes,
                (bytes + packet_payload - 1) / packet_payload, packet_payload, layout.chunk_count);
    return 0;
}

#else

#include <cstdio>

int main() {
    std::printf("rdma is only implemented on Linux\n");
    return 1;
}

#endif  // defined(__linux__)




