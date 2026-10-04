#if defined(__linux__)

#include <CLI/CLI.hpp>
#include <NBIO/Utility/Byte.hpp>
#include <NBIO/Core/Expected.hpp>
#include <NBIO/Core/RdmaAcceptor.hpp>
#include <NBIO/Core/RdmaConnector.hpp>
#include <NBIO/Core/RdmaHeader.hpp>
#include <NBIO/Core/RdmaResourceManager.hpp>
#include <NBIO/Core/SocketAddress.hpp>
#include <NBIO/NBIO/Engine.hpp>
#include <NBIO/NBIO/EpollMultiplexer.hpp>
#include <NBIO/NBIO/NBIO.hpp>
#include <NBIO/NBIO/RdmaAcceptChannel.hpp>
#include <NBIO/NBIO/RdmaConnectChannel.hpp>
#include <NBIO/NBIO/RdmaDeliverService.hpp>
#include <NBIO/NBIO/URingMultiplexer.hpp>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
namespace FCore = NBIO::Core;
namespace NBIO = NBIO::NBIO;

using Clock = std::chrono::steady_clock;

enum Kind : std::uint64_t {
    kStart = 1,
    kDone = 2,
};

struct Report {
    std::uint64_t kind{0};
    std::uint64_t value{0};
    std::uint64_t extra{0};
};

struct Outcome {
    bool ok{false};
    std::size_t bytes{0};
    std::size_t messages{0};
    double seconds{0.0};
    std::uint64_t sent{0};
    std::uint64_t acknowledged{0};
    std::uint64_t taken{0};
    std::string failure;
};

constexpr std::size_t kReportBytes = 3 * sizeof(std::uint64_t);
constexpr std::size_t kPreferredDefaultMessageBytes = 4096;
constexpr std::size_t kDefaultMessages = 200000;

void PutU64(char* out, std::uint64_t value) noexcept {
    const auto ordered = FCore::ToBigEndian(value);
    std::memcpy(out, &ordered, sizeof(ordered));
}

std::uint64_t GetU64(const char* in) noexcept {
    std::uint64_t ordered = 0;
    std::memcpy(&ordered, in, sizeof(ordered));
    return FCore::FromBigEndian(ordered);
}

std::array<char, kReportBytes> EncodeReport(const Report& report) noexcept {
    std::array<char, kReportBytes> message{};
    PutU64(message.data(), report.kind);
    PutU64(message.data() + sizeof(std::uint64_t), report.value);
    PutU64(message.data() + 2 * sizeof(std::uint64_t), report.extra);
    return message;
}

bool DecodeReport(std::span<const char> message, Report& report) noexcept {
    if (message.size() != kReportBytes) {
        return false;
    }
    report.kind = GetU64(message.data());
    report.value = GetU64(message.data() + sizeof(std::uint64_t));
    report.extra = GetU64(message.data() + 2 * sizeof(std::uint64_t));
    return true;
}

double SecondsSince(const Clock::time_point started) noexcept {
    return std::chrono::duration<double>(Clock::now() - started).count();
}

std::unique_ptr<NBIO::Multiplexer> MakeMultiplexer(const std::string& name) {
    if (name == "epoll") {
        return std::make_unique<NBIO::EpollMultiplexer>();
    }
    if (name == "io_uring") {
        return std::make_unique<NBIO::URingMultiplexer>();
    }
    throw std::invalid_argument("--multiplexer must be 'epoll' or 'io_uring'");
}

template <typename Body>
void RunEngine(Body body, const std::string& multiplexer, Outcome& outcome) {
    try {
        NBIO::initialize(MakeMultiplexer(multiplexer));
        body();
    } catch (const std::exception& error) {
        outcome.failure = error.what();
    } catch (...) {
        outcome.failure = "unknown failure";
    }
}

NBIO::Task<FCore::expected<std::span<char>, std::string>> ReceiveOne(NBIO::RdmaDeliverService& service) {
    auto incoming = co_await service.receive();
    if (!incoming) [[unlikely]] {
        co_return FCore::unexpected(incoming.error());
    }
    if (!*incoming) [[unlikely]] {
        co_return FCore::unexpected(std::string{"the link ended"});
    }
    co_return **incoming;
}

NBIO::Task<void> ServerBenchmark(NBIO::RdmaAcceptChannel& channel, NBIO::RdmaDeliverService::Layout layout,
                                 Outcome& outcome) {
    auto admitted = co_await channel.accept();
    if (!admitted) [[unlikely]] {
        outcome.failure = "accept failed: " + admitted.error();
        co_return;
    }

    auto service = std::make_shared<NBIO::RdmaDeliverService>(*admitted, layout);
    if (auto ready = co_await service->handshake(); !ready) [[unlikely]]
    {
        outcome.failure = "handshake failed: " + ready.error();
        co_return;
    }
    service->start();

    auto start_message = co_await ReceiveOne(*service);
    if (!start_message) [[unlikely]] {
        outcome.failure = "start message receive failed: " + start_message.error();
        co_return;
    }
    Report start_report;
    if (!DecodeReport(*start_message, start_report) || start_report.kind != kStart) [[unlikely]] {
        outcome.failure = "invalid start message";
        co_return;
    }
    if (auto released = co_await service->release(*start_message); !released) [[unlikely]]
    {
        outcome.failure = "cannot release start message: " + released.error();
        co_return;
    }

    const std::size_t message_bytes = static_cast<std::size_t>(start_report.value);
    const std::size_t messages = static_cast<std::size_t>(start_report.extra);
    const auto started = Clock::now();

    std::size_t bytes = 0;
    for (std::size_t index = 0; index < messages; ++index) {
        auto payload = co_await ReceiveOne(*service);
        if (!payload) [[unlikely]] {
            outcome.failure = "payload receive failed after " + std::to_string(index) + " messages: " + payload.error();
            co_return;
        }
        if ((*payload).size() != message_bytes) [[unlikely]] {
            outcome.failure = "payload size mismatch at message " + std::to_string(index) + ": got " +
                              std::to_string((*payload).size()) + ", expected " + std::to_string(message_bytes);
            co_return;
        }
        bytes += (*payload).size();
        if (auto released = co_await service->release(*payload); !released) [[unlikely]]
        {
            outcome.failure = "cannot release payload: " + released.error();
            co_return;
        }
    }

    const auto done_message = EncodeReport(Report{.kind = kDone, .value = bytes, .extra = messages});
    if (auto posted = co_await service->send(std::span<const char>(done_message.data(), done_message.size())); !posted)
        [[unlikely]]
    {
        outcome.failure = "done message send failed: " + posted.error();
        co_return;
    }

    outcome.ok = true;
    outcome.bytes = bytes;
    outcome.messages = messages;
    outcome.seconds = SecondsSince(started);
    outcome.sent = service->sent();
    outcome.acknowledged = service->acknowledged();
    outcome.taken = service->taken();
    service->stop();
    co_return;
}

NBIO::Task<void> ClientBenchmark(NBIO::RdmaConnectChannel& channel, const FCore::SocketAddress& peer,
                                 NBIO::RdmaDeliverService::Layout layout, std::size_t message_bytes,
                                 std::size_t messages, Outcome& outcome) {
    auto connected = co_await channel.connect(peer);
    if (!connected) [[unlikely]] {
        outcome.failure = "connect failed: " + connected.error();
        co_return;
    }

    auto service = std::make_shared<NBIO::RdmaDeliverService>(*connected, layout);
    if (auto ready = co_await service->handshake(); !ready) [[unlikely]]
    {
        outcome.failure = "handshake failed: " + ready.error();
        co_return;
    }
    service->start();

    const auto start_message = EncodeReport(Report{.kind = kStart, .value = message_bytes, .extra = messages});
    if (auto announced = co_await service->send(std::span<const char>(start_message.data(), start_message.size()));
        !announced) [[unlikely]]
    {
        outcome.failure = "start message send failed: " + announced.error();
        co_return;
    }

    std::vector<char> payload(message_bytes);
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<char>((index * 31 + 17) & 0xFF);
    }

    const auto started = Clock::now();
    for (std::size_t index = 0; index < messages; ++index) {
        if (auto posted = co_await service->send(payload); !posted) [[unlikely]]
        {
            outcome.failure = "payload send failed after " + std::to_string(index) + " messages: " + posted.error();
            co_return;
        }
    }

    auto done_message = co_await ReceiveOne(*service);
    if (!done_message) [[unlikely]] {
        outcome.failure = "done message receive failed: " + done_message.error();
        co_return;
    }
    Report done_report;
    if (!DecodeReport(*done_message, done_report) || done_report.kind != kDone) [[unlikely]] {
        outcome.failure = "invalid done message";
        co_return;
    }
    if (auto released = co_await service->release(*done_message); !released) [[unlikely]]
    {
        outcome.failure = "cannot release done message: " + released.error();
        co_return;
    }

    const std::size_t bytes = static_cast<std::size_t>(done_report.value);
    const std::size_t received_messages = static_cast<std::size_t>(done_report.extra);
    if (bytes != message_bytes * messages || received_messages != messages) [[unlikely]] {
        outcome.failure = "server reported " + std::to_string(bytes) + " bytes and " +
                          std::to_string(received_messages) + " messages";
        co_return;
    }

    outcome.ok = true;
    outcome.bytes = bytes;
    outcome.messages = messages;
    outcome.seconds = SecondsSince(started);
    outcome.sent = service->sent();
    outcome.acknowledged = service->acknowledged();
    outcome.taken = service->taken();
    service->stop();
    co_return;
}

void PrintOutcome(const char* role, const Outcome& outcome) {
    if (!outcome.ok) [[unlikely]] {
        std::printf("%s FAILED: %s\n", role, outcome.failure.c_str());
        return;
    }
    const auto mib = static_cast<double>(outcome.bytes) / (1024.0 * 1024.0);
    const auto mibps = mib / outcome.seconds;
    const auto mps = static_cast<double>(outcome.messages) / outcome.seconds;
    std::printf(
        "%s bytes=%zu messages=%zu seconds=%.3f throughput=%.2f MiB/s rps=%.0f sent=%llu acknowledged=%llu "
        "taken=%llu\n",
        role, outcome.bytes, outcome.messages, outcome.seconds, mibps, mps,
        static_cast<unsigned long long>(outcome.sent), static_cast<unsigned long long>(outcome.acknowledged),
        static_cast<unsigned long long>(outcome.taken));
}
}  // namespace

int main(int argc, char* argv[]) {
    CLI::App application{"RdmaDeliverService benchmark in server/client mode"};

    std::string mode;
    std::string ip;
    std::string local_ip;
    std::string device;
    std::uint16_t port = 0;
    std::size_t message_bytes = kPreferredDefaultMessageBytes;
    std::size_t messages = kDefaultMessages;
    std::string multiplexer = "epoll";

    application.add_option("--mode", mode, "server or client")->required()->check(CLI::IsMember({"server", "client"}));
    application.add_option("--ip", ip, "server IP address")->required();
    application.add_option("--local-ip", local_ip,
                           "local RDMA IP to bind before connect; useful on multi-host or multi-interface setups");
    application.add_option("--rdma-device", device, "RDMA device name, e.g. siw0")
        ->envname("KVSTORE_RDMA_DEVICE")
        ->required();
    application.add_option("--port", port, "server port")->check(CLI::Range(1, 65535))->required();
    auto* message_bytes_option =
        application.add_option("--message-bytes", message_bytes, "size of each benchmark message in bytes");
    application.add_option("--messages", messages, "number of messages to transfer");
    application.add_option("--multiplexer", multiplexer, "I/O multiplexer: epoll or io_uring")
        ->check(CLI::IsMember({"epoll", "io_uring"}));

    try {
        application.parse(argc, argv);
    } catch (const CLI::ParseError& error) {
        return application.exit(error);
    }

    FCore::RdmaResourceManager resources(device);
    const auto receive_chunk = resources.receive_memory().chunk_size();
    if (receive_chunk == 0) [[unlikely]] {
        std::printf("receive chunk size is zero\n");
        return 1;
    }

    const NBIO::RdmaDeliverService::Layout layout{.chunk_size = receive_chunk,
                                                  .chunk_count = FCore::RdmaConnector::kReceiveChunks};
    const auto default_message_bytes =
        std::min<std::size_t>(kPreferredDefaultMessageBytes, static_cast<std::size_t>(layout.payload_size()));
    if (message_bytes_option->count() == 0) {
        message_bytes = default_message_bytes;
    }
    if (message_bytes == 0 || message_bytes > layout.payload_size()) [[unlikely]] {
        std::printf("--message-bytes must be in [1, %llu], got %zu\n",
                    static_cast<unsigned long long>(layout.payload_size()), message_bytes);
        return 1;
    }
    if (messages == 0) [[unlikely]] {
        std::printf("--messages must be greater than zero\n");
        return 1;
    }

    const auto peer = FCore::SocketAddress::from_v4(ip, port);
    std::printf(
        "mode=%s ip=%s local_ip=%s port=%u device=%s message_bytes=%zu messages=%zu window=%llu multiplexer=%s\n",
        mode.c_str(), ip.c_str(), local_ip.empty() ? "<auto>" : local_ip.c_str(), static_cast<unsigned>(port),
        device.c_str(), message_bytes, messages, static_cast<unsigned long long>(layout.chunk_count),
        multiplexer.c_str());

    Outcome outcome;
    if (mode == "server") {
        FCore::RdmaAcceptor acceptor(resources);
        assert(acceptor.reuse_address(true).has_value());
        const auto bound = acceptor.listen(peer);

        if (!bound) [[unlikely]] {
            std::printf("listen failed on %s:%u: %s\n", ip.c_str(), static_cast<unsigned>(port), bound.error().c_str());
            return 1;
        }
        RunEngine(
            [&] {
                NBIO::RdmaAcceptChannel channel(acceptor, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run(ServerBenchmark(channel, layout, outcome));
            },
            multiplexer, outcome);
    } else {
        RunEngine(
            [&] {
                FCore::RdmaConnector connector(resources);
                if (!local_ip.empty()) {
                    if (const auto bound = connector.bind(FCore::SocketAddress::from_v4(local_ip, 0)); !bound) {
                        throw std::runtime_error("cannot bind local RDMA address: " + bound.error());
                    }
                }
                NBIO::RdmaConnectChannel channel(connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
                NBIO::run(ClientBenchmark(channel, peer, layout, message_bytes, messages, outcome));
            },
            multiplexer, outcome);
    }

    PrintOutcome(mode.c_str(), outcome);
    return outcome.ok ? 0 : 1;
}

#else

#include <cstdio>

int main() {
    std::printf("RDMA is only implemented on Linux\n");
    return 1;
}

#endif  // defined(__linux__)
