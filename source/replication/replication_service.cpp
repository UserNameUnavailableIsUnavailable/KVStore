#include <replication/replication_service.hpp>

#if defined(__linux__)

#include <spdlog/spdlog.h>

#include <resp/resp.hpp>
#include <nbio/async.hpp>
#include <nbio/net.hpp>
#include <nbio/time.hpp>
#include <nbio/utility.hpp>
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace KV {
namespace {
std::uint64_t NewEpoch() {
    std::random_device random;
    auto value = (static_cast<std::uint64_t>(random()) << 32U) ^ random();
    return value ? value : 1;
}

std::string Endpoint(const nbio::net::Address& address) {
    return *address.ip() + ":" + std::to_string(*address.port());
}

std::uint64_t FileBytes(const std::filesystem::path& file) noexcept {
    std::error_code error;
    const auto size = std::filesystem::file_size(file, error);
    return error ? 0 : static_cast<std::uint64_t>(size);
}

bool ParseUnsigned(std::string_view text, std::uint64_t& value) noexcept {
    return Replication::Unsigned(text, value);
}

bool SplitEndpoint(std::string_view text, std::string& ip, std::uint16_t& port) noexcept {
    const auto colon = text.rfind(':');
    std::uint64_t value = 0;
    if (colon == std::string_view::npos || !ParseUnsigned(text.substr(colon + 1), value) || value == 0 ||
        value > 65535) {
        return false;
    }
    ip.assign(text.substr(0, colon));
    port = static_cast<std::uint16_t>(value);
    return !ip.empty();
}

nbio::async::Task<bool> SendAll(nbio::net::TcpSessionService& session, std::string_view bytes) {
    while (!bytes.empty()) {
        const auto sent = co_await session.Send(std::span<const char>(bytes.data(), bytes.size()));
        if (!sent || *sent == 0) {
            co_return false;
        }
        bytes.remove_prefix(*sent);
    }
    co_return true;
}

// A RESP array of bulk strings: the only shape the master's command receiver
// accepts for REPLID / FOLLOW.
std::string RespCommand(std::initializer_list<std::string_view> parts) {
    std::string out = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto part : parts) {
        out += "$" + std::to_string(part.size()) + "\r\n";
        out.append(part.data(), part.size());
        out += "\r\n";
    }
    return out;
}

nbio::async::Task<std::optional<std::string>> ReadLine(nbio::net::TcpSessionService& session,
                                                       memory::Buffer& buffer) {
    while (true) {
        const auto readable = buffer.readable_span();
        const std::string_view view(readable.data(), readable.size());
        if (const auto end = view.find("\r\n"); end != std::string_view::npos) {
            std::string line(view.substr(0, end));
            buffer.Consume(end + 2);
            co_return line;
        }
        if (buffer.writable_size() == 0 && !buffer.Reserve(buffer.capacity() * 2)) {
            co_return std::nullopt;
        }
        const auto received = co_await session.Receive(buffer.writable_span());
        if (!received || *received == 0) {
            co_return std::nullopt;
        }
        buffer.Commit(*received);
    }
}

// Reads exactly `length` bytes of a frame body: whatever already sits in `buffer`,
// then more off the wire.
nbio::async::Task<std::optional<std::string>> ReadExact(nbio::net::TcpSessionService& session,
                                                        memory::Buffer& buffer, std::size_t length) {
    std::string out;
    out.reserve(length);
    const auto readable = buffer.readable_span();
    const auto taken = std::min(length, readable.size());
    out.append(readable.data(), taken);
    buffer.Consume(taken);
    std::array<char, 65536> chunk;
    while (out.size() < length) {
        const auto want = std::min(chunk.size(), length - out.size());
        const auto received = co_await session.Receive(std::span<char>(chunk.data(), want));
        if (!received || *received == 0) {
            co_return std::nullopt;
        }
        out.append(chunk.data(), *received);
    }
    co_return out;
}
}  // namespace

// The replica's view of a pushed stream (from the master or a broker): a
// long-lived registration that buffers the stream while the snapshot is fetched,
// then replays it in order.
class ReplicationService::PushFeed {
   public:
    // Master: we register with REPLICA and the peer answers +CONTINUE with the
    // epoch and the offset where the stream starts.
    // Broker: the broker has already registered on our behalf and forwards
    // blindly, so we are attached mid-stream and must find our own start.
    enum class Mode { Master, Broker };

    // Why Replay stopped; the caller decides whether to keep the registration.
    enum class Result { Ok, Gap, EpochMismatch, Dead };

    PushFeed(std::string endpoint, std::size_t capacity, Mode mode)
        : endpoint_(std::move(endpoint)), capacity_(capacity), mode_(mode) {}

    ~PushFeed() {
        if (session_) {
            session_->connector().Shutdown();
        }
        changed_.NotifyAll();
    }

    PushFeed(const PushFeed&) = delete;
    PushFeed& operator=(const PushFeed&) = delete;

    std::uint64_t epoch() const noexcept { return epoch_; }
    bool dead() const noexcept { return dead_; }

    nbio::async::Task<bool> Open() {
        std::string ip;
        std::uint16_t port = 0;
        if (!SplitEndpoint(endpoint_, ip, port)) {
            spdlog::warn("replication: invalid broker endpoint '{}'", endpoint_);
            co_return false;
        }
        const auto address = nbio::net::Address::FromIPv4(ip, port);
        if (!address) {
            co_return false;
        }
        auto connected = co_await nbio::net::TcpConnectService{}.Connect(*address);
        if (!connected) {
            spdlog::warn("replication: connecting to broker {} failed: {}", endpoint_, connected.error().message());
            co_return false;
        }
        session_ = *connected;
        if (mode_ == Mode::Broker) {
            // No handshake: the broker forwards whatever the master pushes next.
            nbio::async::Spawn(ReadAll());
            spdlog::info("replication: attached to broker {}", endpoint_);
            co_return true;
        }
        // Register for pushes: no offset, the peer decides where the stream starts.
        if (!co_await SendAll(*session_, RespCommand({"REPLICA"}))) {
            co_return false;
        }
        auto greeting = co_await ReadLine(*session_, pending_buffer_);
        // +CONTINUE <epoch> <head>
        std::uint64_t head = 0;
        std::uint64_t epoch = 0;
        if (!greeting || !greeting->starts_with("+CONTINUE ")) {
            spdlog::warn("replication: {} refused the replica registration", endpoint_);
            co_return false;
        }
        const std::string_view rest = std::string_view(*greeting).substr(std::string_view("+CONTINUE ").size());
        const auto space = rest.find(' ');
        if (space == std::string_view::npos || !ParseUnsigned(rest.substr(0, space), epoch) ||
            !ParseUnsigned(rest.substr(space + 1), head)) {
            co_return false;
        }
        epoch_ = epoch;
        received_ = head;
        started_ = true;  // the greeting is authoritative, every frame must follow it
        progress_ = std::chrono::steady_clock::now();
        nbio::async::Spawn(ReadAll());
        spdlog::info("replication: registered with {} for pushes at offset {}", endpoint_, head);
        co_return true;
    }

    // Applies every buffered or live record with offset > `applied`, in order.
    // `expected_epoch` is the incarnation the current snapshot came from; the
    // stream is only trusted once its epoch is known and matches.
    nbio::async::Task<Result> Replay(std::uint64_t applied, std::uint64_t expected_epoch,
                                     std::function<bool(std::string_view)> apply) {
        while (true) {
            co_await changed_.wait([this] { return dead_ || !records_.empty(); });
            if (records_.empty()) {
                if (dead_) {
                    co_return Result::Dead;
                }
                continue;
            }
            if (started_ && epoch_ != expected_epoch) {
                spdlog::warn("replication: pushed stream epoch {} is not the snapshot epoch {}", epoch_,
                             expected_epoch);
                co_return Result::EpochMismatch;
            }
            while (!records_.empty()) {
                auto& front = records_.front();
                if (front.first <= applied) {
                    bytes_ -= front.second.size() + sizeof(std::pair<std::uint64_t, std::string>);
                    records_.pop_front();
                    continue;
                }
                if (front.first != applied + 1) {
                    spdlog::warn("replication: broker stream gap: have {} awaiting {}", front.first, applied + 1);
                    co_return Result::Gap;
                }
                if (!apply(front.second)) {
                    co_return Result::Dead;
                }
                applied = front.first;
                bytes_ -= front.second.size() + sizeof(std::pair<std::uint64_t, std::string>);
                records_.pop_front();
            }
            if (dead_) {
                co_return Result::Dead;
            }
        }
    }

   private:
    nbio::async::Task<void> ReadAll() {
        try {
            while (true) {
                auto header = co_await ReadLine(*session_, pending_buffer_);
                if (!header) {
                    throw std::runtime_error("broker closed");
                }
                const auto parsed = Replication::ParseFrame(*header);
                if (!started_) {
                    // Attached mid-stream: the leading bytes are whatever was in
                    // flight, so skip lines until one parses as a frame header.
                    // A false positive cannot corrupt state: Replay rejects any
                    // first offset that does not follow the snapshot.
                    if (!parsed) {
                        continue;
                    }
                    epoch_ = parsed->epoch;
                    spdlog::info("replication: broker stream starts at epoch {} offset {}", epoch_,
                                 parsed->end_offset);
                } else if (!parsed || !Replication::Follows(*parsed, epoch_, received_)) {
                    throw std::runtime_error("broker stream is discontinuous");
                }
                if (parsed->length) {
                    const auto cost = static_cast<std::size_t>(parsed->length) + sizeof(std::pair<std::uint64_t, std::string>);
                    if (cost > capacity_ || bytes_ > capacity_ - cost) {
                        throw std::runtime_error("replica broker buffer exhausted");
                    }
                    auto payload = co_await ReadExact(*session_, pending_buffer_, static_cast<std::size_t>(parsed->length));
                    if (!payload) {
                        throw std::runtime_error("broker frame truncated");
                    }
                    bytes_ += cost;
                    records_.emplace_back(parsed->end_offset, std::move(*payload));
                    received_ = parsed->end_offset;
                    changed_.NotifyAll();
                } else if (!started_) {
                    received_ = parsed->end_offset;
                }
                started_ = true;
                progress_ = std::chrono::steady_clock::now();
            }
        } catch (const std::exception& error) {
            if (!dead_) {
                spdlog::warn("replication: broker stream ended: {}", error.what());
            }
            dead_ = true;
            changed_.NotifyAll();
        }
    }

    std::string endpoint_;
    std::size_t capacity_;
    Mode mode_;
    std::shared_ptr<nbio::net::TcpSessionService> session_;
    memory::Buffer pending_buffer_{1U << 14U, 1U << 22U};
    std::deque<std::pair<std::uint64_t, std::string>> records_;
    std::size_t bytes_ = 0;
    std::uint64_t epoch_ = 0;
    std::uint64_t received_ = 0;
    bool started_ = false;  // epoch_ and received_ are known
    bool dead_ = false;
    std::chrono::steady_clock::time_point progress_{};
    nbio::notification::ConditionVariable changed_;
};

ReplicationService::ReplicationService(Options options, Host host)
    : options_(std::move(options)),
      host_(std::move(host)),
      replid_(options_.epoch ? options_.epoch : NewEpoch()),
      backlog_(options_.backlog_bytes) {
    if (options_.rdma_device.empty()) {
        throw std::invalid_argument(
            "replication: replication needs an rdma device; pass --rdma-device or 'config rdma_device <name>'");
    }
    if (!options_.listen_address.empty() &&
        (options_.listen_address == "0.0.0.0" || options_.listen_address == "::")) {
        throw std::invalid_argument("replication: serving replicas needs the address of an rdma device, not '" +
                                    options_.listen_address + "'");
    }
    if (options_.listen_address.find(':') != std::string::npos) {
        throw std::invalid_argument("replication: '" + options_.listen_address +
                                    "' is not an address to serve replicas from; --replication-ip takes the address alone");
    }
    if (!options_.broker_endpoint.empty()) {
        std::string ip;
        std::uint16_t port = 0;
        if (!SplitEndpoint(options_.broker_endpoint, ip, port) || !nbio::net::Address::FromIPv4(ip, port)) {
            throw std::invalid_argument("replication: --broker wants IPv4:port, got '" + options_.broker_endpoint + "'");
        }
    }
}

ReplicationService::~ReplicationService() noexcept = default;

void ReplicationService::Start() {}

void ReplicationService::release_snapshot() noexcept {
    active_dump_.reset();
    snapshot_busy_ = false;
    snapshot_ready_.NotifyOne();
}

void ReplicationService::AbortDump() noexcept {
    if (active_dump_) {
        release_snapshot();
    }
}

void ReplicationService::record(std::string_view payload) {
    if (payload.empty()) {
        return;
    }
    const auto offset = host_.offset();
    try {
        backlog_.Append(offset, payload);
    } catch (const std::exception& error) {
        // A gap means history is unusable from here on: restart the window so a
        // follower that is behind is told to resynchronise instead of skipping.
        spdlog::warn("replication: backlog discontinuity at offset {}: {}", offset, error.what());
        backlog_.Reset(offset);
    }
    backlog_changed_.NotifyAll();
}

bool ReplicationService::apply_payload(std::string_view payload) {
    memory::Buffer command(payload.size(), payload.size());
    if (!command.Write(payload.data(), payload.size())) {
        throw std::runtime_error("buffering replicated command");
    }
    auto decoder = RESP::Decode(command);
    if (decoder.poll() != RESP::DecodeStatus::kComplete || !decoder.result().object ||
        !command.readable_span().empty()) {
        throw std::runtime_error("invalid replicated RESP command");
    }
    const auto validated = ValidateCommand(*decoder.result().object);
    if (!validated || !IsWriteCommand(validated.command->type)) {
        throw std::runtime_error("invalid replicated write command");
    }
    return host_.apply(*validated.command);
}

nbio::async::Task<nbio::utility::expected<ReplicationService::SyncOffer, std::error_code>>
ReplicationService::OfferDump() {
    co_await snapshot_ready_.wait([this] { return !snapshot_busy_; });
    snapshot_busy_ = true;

    const std::uint64_t taken_at = host_.offset();
    auto pending = host_.snapshot();
    // The backlog is *not* reset here. A follower (or the broker behind one) may
    // already be streaming from an earlier offset, and a snapshot must not tear
    // that stream: the offset in the FULLSYNC answer is enough to tell a replica
    // where to start replaying the buffer it already has.

    if (!co_await pending.Wait()) [[unlikely]] {
        spdlog::warn("replication: this master could not write a snapshot");
        release_snapshot();
        co_return nbio::utility::unexpected(make_error_code(std::errc::io_error));
    }

    const auto file = host_.snapshot_file();
    const std::uint64_t bytes = FileBytes(file);
    if (file.empty() || bytes == 0) [[unlikely]] {
        spdlog::warn("replication: this master has no snapshot file to serve");
        release_snapshot();
        co_return nbio::utility::unexpected(make_error_code(std::errc::no_such_file_or_directory));
    }

    std::uint16_t rdma_port = 0;
    try {
        auto mapping = std::make_unique<nbio::fs::FileView>(file);
        if (mapping->size() != bytes) [[unlikely]] {
            throw std::runtime_error("the snapshot file changed size while being mapped");
        }
        const auto page = nbio::net::RdmaResource::page_size();
        auto* receive = static_cast<char*>(std::aligned_alloc(page, page));
        if (!receive) [[unlikely]] throw std::bad_alloc();
        std::unique_ptr<char, decltype(&std::free)> receive_arena{receive, &std::free};

        auto resource = std::make_unique<nbio::net::RdmaResource>(
            options_.rdma_device,
            std::span<const char>(static_cast<const char*>(mapping->data()), mapping->size()),
            std::span<char>(receive_arena.get(), page));

        std::unique_ptr<nbio::net::RdmaListenService> listener;
        for (int attempt = 0; attempt < 16; ++attempt) {
            listener = std::make_unique<nbio::net::RdmaListenService>(*resource);
            const auto address = nbio::net::Address::FromIPv4(options_.listen_address, 0);
            if (!address) throw std::system_error(address.error(), "invalid replication listen address");
            const auto bound = listener->Bind(*address);
            if (bound) {
                const auto listening = listener->Listen();
                if (listening) {
                    break;
                }
                if (listening.error().value() != EADDRINUSE) {
                    throw std::system_error(listening.error(), "failed to listen on the rdma address");
                }
            } else if (bound.error().value() != EADDRINUSE) {
                throw std::system_error(bound.error(), "failed to bind the rdma listener");
            }
            listener.reset();
        }
        if (!listener) [[unlikely]] {
            throw std::runtime_error("the rdma listener could not claim a free port");
        }
        rdma_port = listener->listener().BoundPort();
        if (rdma_port == 0) {
            throw std::runtime_error("the rdma listener bound no port");
        }

        active_dump_.emplace(ActiveDump{
            .mapping = std::move(mapping),
            .receive_arena = std::move(receive_arena),
            .resource = std::move(resource),
            .listener = std::move(listener),
            .bytes = bytes,
        });
    } catch (const std::exception& error) {
        spdlog::warn("replication: preparing the dump failed: {}", error.what());
        release_snapshot();
        co_return nbio::utility::unexpected(make_error_code(std::errc::io_error));
    }

    SyncOffer offer{
        .bytes = bytes,
        .rdma_ip = options_.listen_address,
        .rdma_port = rdma_port,
        .offset = taken_at,
        .epoch = replid_,
    };
    spdlog::info("replication: offered '{}' ({} bytes) at offset {} on rdma://{}:{}", file.string(), bytes, taken_at,
                 options_.listen_address, rdma_port);
    co_return offer;
}

nbio::async::Task<void> ReplicationService::ServeDump() {
    if (!active_dump_) [[unlikely]] {
        co_return;
    }
    struct Release {
        ReplicationService& service;
        ~Release() { service.release_snapshot(); }
    } release{*this};

    auto& dump = *active_dump_;
    auto accepted = co_await dump.listener->Accept();
    if (!accepted) [[unlikely]] {
        spdlog::warn("replication: accepting the dump connection failed: {}", accepted.error().message());
        co_return;
    }

    auto session = std::make_shared<nbio::net::RdmaSessionService>(std::move(*accepted));
    if (auto up = co_await session->Establish(); !up) [[unlikely]] {
        spdlog::warn("replication: establishing the dump link failed: {}", up.error().message());
        co_return;
    }

    const std::span<const char> bytes(static_cast<const char*>(dump.mapping->data()), dump.mapping->size());
    const auto seq = session->SubmitSend(bytes);
    if (!seq) [[unlikely]] {
        spdlog::warn("replication: posting the dump failed: {}", seq.error().message());
        co_return;
    }
    const auto mark = co_await session->WaitSend(*seq);
    if (!mark) [[unlikely]] {
        spdlog::warn("replication: waiting for the dump failed: {}", mark.error().message());
        co_return;
    }
    // A send result hands the posted buffer back to us; we only look, the mapping
    // stays owned by the FileView.
    if (const auto retired = session->PeekSend(*seq); !retired) [[unlikely]] {
        spdlog::warn("replication: reading back the dump completion failed: {}", retired.error().message());
    }
    session->CompleteSend(*seq);
    spdlog::info("replication: sent the dump ({} bytes)", dump.bytes);
    co_return;
}

nbio::async::Task<void> ReplicationService::follow(nbio::net::TcpSessionService& session) {
    // Push, not pull: the follower does not name an offset. The master starts it
    // at the head it has now and copies every command it executes from there.
    std::uint64_t cursor = backlog_.head();
    if (!co_await SendAll(session, "+CONTINUE " + std::to_string(replid_) + " " + std::to_string(cursor) + "\r\n")) {
        co_return;
    }

    while (true) {
        bool sent = false;
        while (const auto* record = backlog_.Next(cursor)) {
            // Copy out before awaiting: the backlog may evict this record while a
            // send is suspended.
            std::string payload = record->payload;
            const std::uint64_t end = record->offset;
            if (!co_await SendAll(session, "@" + std::to_string(replid_) + " " + std::to_string(end) + " " +
                                               std::to_string(payload.size()) + "\r\n")) {
                co_return;
            }
            if (!co_await SendAll(session, payload)) {
                co_return;
            }
            cursor = end;
            sent = true;
        }
        // A follower that stopped draining fell out of the retained window: close
        // it, and it will resynchronise from a fresh snapshot.
        if (!backlog_.Contains(cursor)) {
            co_return;
        }
        if (sent) {
            continue;
        }
        co_await nbio::async::WhenAny(
            backlog_changed_.wait([this, cursor] { return !backlog_.Contains(cursor) || backlog_.head() != cursor; }),
            nbio::time::SystemTimeService{}.Sleep(std::chrono::seconds(1)));
        if (!backlog_.Contains(cursor)) {
            co_return;
        }
        if (backlog_.head() == cursor) {
            // Idle heartbeat: a length-0 frame at the current offset.
            if (!co_await SendAll(session, "@" + std::to_string(replid_) + " " + std::to_string(cursor) + " 0\r\n")) {
                co_return;
            }
        }
    }
}

bool ReplicationService::slave_of(const nbio::net::Address& master) {
    bool expected = false;
    if (!following_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false;
    }
    spdlog::info("replication: following master {} as a replica", Endpoint(master));
    nbio::async::Spawn(follow_forever(master));
    return true;
}

nbio::async::Task<std::optional<std::uint64_t>> ReplicationService::sync_once(nbio::net::Address master) {
    auto session = co_await nbio::net::TcpConnectService{}.Connect(master);
    if (!session) {
        spdlog::warn("replication: connecting to {} failed: {}", Endpoint(master), session.error().message());
        co_return std::nullopt;
    }
    constexpr std::string_view sync_command = "*1\r\n$4\r\nSYNC\r\n";
    if (!co_await SendAll(**session, sync_command)) {
        co_return std::nullopt;
    }

    memory::Buffer buffer(1U << 14U, 1U << 22U);
    auto greeting = co_await ReadLine(**session, buffer);
    // +FULLSYNC <bytes> <ip>:<port> <offset> <epoch>
    std::uint64_t bytes = 0;
    std::uint64_t offset = 0;
    std::uint64_t epoch = 0;
    std::string rdma_ip;
    std::uint16_t rdma_port = 0;
    if (!greeting || !greeting->starts_with("+FULLSYNC ")) {
        spdlog::warn("replication: {} answered '{}' where a FULLSYNC was owed", Endpoint(master),
                     greeting ? *greeting : std::string("nothing"));
        co_return std::nullopt;
    }
    {
        std::string_view rest(*greeting);
        rest.remove_prefix(std::string_view("+FULLSYNC ").size());
        std::array<std::string_view, 4> fields{};
        for (std::size_t i = 0; i + 1 < fields.size(); ++i) {
            const auto space = rest.find(' ');
            if (space == std::string_view::npos) {
                co_return std::nullopt;
            }
            fields[i] = rest.substr(0, space);
            rest.remove_prefix(space + 1);
        }
        fields.back() = rest;
        if (!ParseUnsigned(fields[0], bytes) || bytes == 0 || !SplitEndpoint(fields[1], rdma_ip, rdma_port) ||
            !ParseUnsigned(fields[2], offset) || !ParseUnsigned(fields[3], epoch)) {
            spdlog::warn("replication: {} sent an unreadable FULLSYNC: '{}'", Endpoint(master), *greeting);
            co_return std::nullopt;
        }
    }
    const auto rdma_address = nbio::net::Address::FromIPv4(rdma_ip, rdma_port);
    if (!rdma_address) {
        co_return std::nullopt;
    }

    const auto page = nbio::net::RdmaResource::page_size();
    const std::size_t rounded = static_cast<std::size_t>(((bytes + page - 1) / page) * page);
    auto* receive_memory = static_cast<char*>(std::aligned_alloc(page, rounded));
    auto* send_memory = static_cast<char*>(std::aligned_alloc(page, page));
    if (!receive_memory || !send_memory) [[unlikely]] {
        std::free(receive_memory);
        std::free(send_memory);
        co_return std::nullopt;
    }
    struct FreeBuffers {
        char* receive;
        char* send;
        ~FreeBuffers() {
            std::free(receive);
            std::free(send);
        }
    } buffers{receive_memory, send_memory};

    nbio::net::RdmaResource resource{options_.rdma_device, std::span<const char>(send_memory, page),
                                     std::span<char>(receive_memory, rounded)};
    nbio::net::RdmaConnectService connector{resource, *rdma_address};
    auto connected = co_await connector.Connect();
    if (!connected) [[unlikely]] {
        spdlog::warn("replication: connecting the dump link to {} failed: {}", Endpoint(*rdma_address),
                     connected.error().message());
        co_return std::nullopt;
    }
    auto rdma_session = std::make_shared<nbio::net::RdmaSessionService>(std::move(*connected));
    const auto receive_seq = rdma_session->SubmitReceive(std::span<char>(receive_memory, rounded));
    if (!receive_seq) [[unlikely]] {
        co_return std::nullopt;
    }
    if (auto up = co_await rdma_session->Establish(); !up) [[unlikely]] {
        spdlog::warn("replication: establishing the dump link failed: {}", up.error().message());
        co_return std::nullopt;
    }
    const auto mark = co_await rdma_session->WaitReceive(*receive_seq);
    if (!mark) [[unlikely]] {
        spdlog::warn("replication: receiving the dump failed: {}", mark.error().message());
        co_return std::nullopt;
    }
    const auto received = rdma_session->PeekReceive(*receive_seq);
    if (!received) [[unlikely]] {
        co_return std::nullopt;
    }

    const auto file = host_.snapshot_file();
    const auto incoming = std::filesystem::path(file).concat(".incoming");
    {
        std::ofstream sink(incoming, std::ios::binary | std::ios::trunc);
        if (!sink) [[unlikely]] {
            spdlog::warn("replication: cannot open '{}'", incoming.string());
            rdma_session->CompleteReceive(*receive_seq);
            co_return std::nullopt;
        }
        sink.write(received->buffer.data(), static_cast<std::streamsize>(received->bytes));
        sink.close();
        if (!sink) [[unlikely]] {
            std::error_code error;
            std::filesystem::remove(incoming, error);
            rdma_session->CompleteReceive(*receive_seq);
            co_return std::nullopt;
        }
    }
    rdma_session->CompleteReceive(*receive_seq);
    std::error_code rename_error;
    std::filesystem::rename(incoming, file, rename_error);
    if (rename_error) [[unlikely]] {
        spdlog::warn("replication: renaming the dump failed: {}", rename_error.message());
        co_return std::nullopt;
    }
    if (!host_.restore(file)) [[unlikely]] {
        spdlog::warn("replication: the dump checksum or restore failed");
        co_return std::nullopt;
    }
    master_epoch_ = epoch;
    spdlog::info("replication: loaded a {} byte dump at offset {} from {}", received->bytes, offset,
                 Endpoint(master));
    // The master TCP connection is dropped here: the pushed stream comes from the
    // broker, or from the master itself when no broker is configured.
    co_return offset;
}

nbio::async::Task<void> ReplicationService::follow_forever(nbio::net::Address master) {
    // Direct replication pushes from the master; with a broker configured, the
    // broker is the push source instead and has done the registration itself.
    const bool via_broker = !options_.broker_endpoint.empty();
    const std::string stream_endpoint = via_broker ? options_.broker_endpoint : Endpoint(master);
    while (is_replica()) {
        // Register for pushes before the snapshot is requested, so the buffer
        // covers the window the dump is taken in.
        if (!feed_ || feed_->dead()) {
            feed_ = std::make_unique<PushFeed>(stream_endpoint, options_.backlog_bytes,
                                               via_broker ? PushFeed::Mode::Broker : PushFeed::Mode::Master);
            if (!co_await feed_->Open()) {
                feed_.reset();
                co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::seconds(1));
                continue;
            }
        }

        std::optional<std::uint64_t> applied;
        try {
            applied = co_await sync_once(master);
        } catch (const std::exception& error) {
            spdlog::warn("replication: sync failed: {}", error.what());
        }
        if (!applied) {
            co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::seconds(1));
            continue;
        }

        host_.link_changed(true);
        const auto result = co_await feed_->Replay(*applied, master_epoch_, [this](std::string_view payload) {
            return apply_payload(payload);
        });
        host_.link_changed(false);
        if (result == PushFeed::Result::EpochMismatch) {
            // The pushed stream is from a different master incarnation than the
            // snapshot; start over with a fresh registration and snapshot.
            feed_.reset();
            continue;
        }
        if (result != PushFeed::Result::Ok) {
            co_await nbio::time::SystemTimeService{}.Sleep(std::chrono::seconds(1));
        }
    }
    co_return;
}
}  // namespace KV

#endif  // defined(__linux__)
