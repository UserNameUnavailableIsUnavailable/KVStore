#include "ReplicationService.hpp"

#if defined(__linux__)

#include <spdlog/spdlog.h>

#include <Application/RESP/RESP.hpp>
#include <NBIO/Utility/Buffer.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/NBIO.hpp>
#include <NBIO/Time/SystemTimeService.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace KV {
namespace {
namespace Core = NBIO::Core;

// How much of a snapshot is read off the disk at a time on its way out.
constexpr std::size_t kSnapshotBlockBytes = 64U << 10U;

// Where a replication id comes from: an integer, so that two masters in one
// process -- and a master that starts again -- do not claim the same one.
std::atomic<std::uint64_t> next_replid{1};

// A reply line, with the CRLF the wire wants and the caller should not have to
// remember.
std::string Reply(std::string_view line) noexcept { return std::string(line) + "\r\n"; }

std::string FullResync(std::uint64_t replid, std::uint64_t offset) noexcept {
    return Reply("+FULLRESYNC " + std::to_string(replid) + " " + std::to_string(offset));
}

std::string Continue(std::uint64_t replid) noexcept { return Reply("+CONTINUE " + std::to_string(replid)); }

// A bulk string's header: the length, and the CRLF that says the bytes follow.
// The bytes themselves are not terminated, because the length is what says where
// they end -- which is the whole point of sending a snapshot this way.
std::string BulkHeader(std::uint64_t bytes) noexcept { return "$" + std::to_string(bytes) + "\r\n"; }

// PSYNC, as the replica writes it: an array of three bulk strings, which is what
// a command is on the wire. The first one starts an exchange and the second one
// rejoins it, and the two differ only in what they say about where the sender is.
std::string EncodePSYNC(std::string_view replid, std::string_view offset) noexcept {
    return "*3\r\n$5\r\nPSYNC\r\n$" + std::to_string(replid.size()) + "\r\n" + std::string(replid) + "\r\n$" +
           std::to_string(offset.size()) + "\r\n" + std::string(offset) + "\r\n";
}

bool ParseUnsigned(std::string_view text, std::uint64_t& value) noexcept {
    if (text.empty()) {
        return false;
    }
    std::uint64_t parsed = 0;
    for (const char digit : text) {
        if (digit < '0' || digit > '9') {
            return false;
        }
        parsed = parsed * 10 + static_cast<std::uint64_t>(digit - '0');
    }
    value = parsed;
    return true;
}

std::uint64_t FileBytes(const std::filesystem::path& file) noexcept {
    std::error_code error;
    const auto size = std::filesystem::file_size(file, error);
    return error ? 0 : static_cast<std::uint64_t>(size);
}

std::string Endpoint(const NBIO::Net::SocketAddress& address) { return address.ip() + ":" + std::to_string(address.port()); }

// A RESP byte stream over a delivery link.
//
// The link hands over payloads of whatever size the peer cut them at, so a
// reader that wants a line, a count or a command has to keep what has arrived
// until it has enough of it: this is the buffer that does that. The shapes the
// protocol needs are lines -- the numbers and the header of a snapshot -- and
// commands, which are arrays of bulk strings.
class LinkStream {
   public:
    LinkStream(NBIO::RDMA::RdmaDeliverService& link, NBIO::Utility::Buffer& buffer) : link_(link), buffer_(buffer) {}

    // One CRLF-terminated line, without the terminator. Nothing when the link
    // ended before one arrived.
    NBIO::Async::Task<NBIO::Runtime, std::optional<std::string>> line() {
        while (true) {
            const std::span<const char> readable = buffer_.readable_span();
            const std::string_view view(readable.data(), readable.size());
            if (const auto end = view.find("\r\n"); end != std::string_view::npos) {
                std::string found(view.substr(0, end));
                buffer_.consume(end + 2);
                co_return found;
            }
            if (!co_await more()) {
                co_return std::nullopt;
            }
        }
    }

    // One command, as its words. Nothing when the link ended, and nothing when
    // what arrived was not a command -- which for either end is the same thing
    // as the peer having stopped speaking the protocol.
    NBIO::Async::Task<NBIO::Runtime, std::optional<std::vector<std::string>>> command() {
        auto header = co_await line();
        if (!header || header->empty() || header->front() != '*') [[unlikely]] {
            co_return std::nullopt;
        }
        std::uint64_t count = 0;
        if (!ParseUnsigned(std::string_view(*header).substr(1), count)) [[unlikely]] {
            co_return std::nullopt;
        }

        std::vector<std::string> words;
        words.reserve(static_cast<std::size_t>(count));
        for (std::uint64_t index = 0; index < count; ++index) {
            auto length = co_await line();
            if (!length || length->empty() || length->front() != '$') [[unlikely]] {
                co_return std::nullopt;
            }
            std::uint64_t bytes = 0;
            if (!ParseUnsigned(std::string_view(*length).substr(1), bytes)) [[unlikely]] {
                co_return std::nullopt;
            }
            // The argument and its terminating CRLF both have to be here before
            // any of it is handed over, or the terminator would be read as the
            // beginning of the next one.
            while (buffer_.readable_span().size() < bytes + 2) {
                if (!co_await more()) {
                    co_return std::nullopt;
                }
            }
            const std::span<const char> readable = buffer_.readable_span();
            words.emplace_back(readable.data(), static_cast<std::size_t>(bytes));
            buffer_.consume(static_cast<std::size_t>(bytes) + 2);
        }
        co_return words;
    }

    // Exactly `bytes`, in whatever pieces they arrive in. False when the link
    // ended first.
    NBIO::Async::Task<NBIO::Runtime, bool> take(std::uint64_t bytes, std::function<bool(std::span<const char>)> sink) {
        while (bytes > 0) {
            const std::span<const char> readable = buffer_.readable_span();
            if (!readable.empty()) {
                const auto taken = static_cast<std::size_t>(std::min<std::uint64_t>(bytes, readable.size()));
                if (!sink(std::span<const char>(readable.data(), taken))) [[unlikely]] {
                    co_return false;
                }
                buffer_.consume(taken);
                bytes -= taken;
                continue;
            }
            if (!co_await more()) [[unlikely]] {
                co_return false;
            }
        }
        co_return true;
    }

    // One more payload into the buffer, and its chunk straight back. False when
    // the link is over. Public because a decoder driven by this stream is fed by
    // the same call, out of the same buffer.
    NBIO::Async::Task<NBIO::Runtime, bool> more() {
        auto incoming = co_await link_.receive();
        if (!incoming || !*incoming) [[unlikely]] {
            co_return false;
        }
        const std::span<char> payload = **incoming;
        // Copied first and given back second: what is released is what the
        // receive was handed, and after that it is no longer ours to read.
        const bool written = buffer_.write(payload.data(), payload.size());
        const auto released = co_await link_.release(payload);
        if (!written || !released) [[unlikely]] {
            co_return false;
        }
        co_return true;
    }

   private:
    NBIO::RDMA::RdmaDeliverService& link_;
    NBIO::Utility::Buffer& buffer_;
};
}  // namespace

ReplicationService::ReplicationService(Options options, Host host)
    : options_(std::move(options)),
      host_(std::move(host)),
      replid_(next_replid.fetch_add(1, std::memory_order_relaxed)) {
    // Replication runs on RDMA, and only on RDMA: there is no TCP path, so an
    // instance asked to serve replicas without a device named would come up
    // unable to serve them. Whether an instance will follow a master is not
    // known yet -- SLAVEOF is what decides that, and it arrives later -- so a
    // service with no listener is built either way.
    if (options_.listen_port != 0 && options_.rdma_device.empty()) {
        throw std::invalid_argument(
            "replication: replication needs an RDMA device; pass --rdma-device or "
            "'config rdma_device <name>'");
    }
    // A wildcard address is accepted by rdma_bind_addr and leaves the id with no
    // device, so a listener asked to serve replicas from one has nothing to
    // serve them from. Refuse it while the reason is still known.
    if (options_.listen_port != 0 &&
        (options_.listen_address.empty() || options_.listen_address == "0.0.0.0" || options_.listen_address == "::")) {
        throw std::invalid_argument("replication: serving replicas needs the address of an RDMA device, not '" +
                                    options_.listen_address + "'");
    }
    // An address goes in one option and the port in another, and `ip:port` in the
    // address is the way that gets written by mistake. Say which option takes
    // what, whether or not a listener is being started: a setting that is wrong
    // is worth knowing about even when nothing is reading it yet.
    if (options_.listen_address.find(':') != std::string::npos) {
        throw std::invalid_argument("replication: '" + options_.listen_address +
                                    "' is not an address to serve replicas from; --replication-ip takes the "
                                    "address alone and --replication-port the port");
    }
    // The device is opened last, once every option is known to be usable: a name
    // that is wrong, or a device that cannot be opened, is a server that refuses
    // to start rather than a replication link that fails later. Every connection
    // the service makes borrows what is opened here.
    if (!options_.rdma_device.empty()) {
        resources_ = std::make_shared<NBIO::RDMA::RdmaResourceManager>(options_.rdma_device);
    }
}

ReplicationService::~ReplicationService() noexcept = default;

NBIO::RDMA::RdmaDeliverService::Layout ReplicationService::link_layout() const {
    // What this end can take: its receive chunks, and as many packets in flight
    // as the connection posts receives for. The pool holds thousands of chunks
    // and says nothing about how many of them the device has been handed, so the
    // count is the connector's depth rather than the pool's size -- a window
    // wider than what is posted is one the peer fills and then fails on.
    return NBIO::RDMA::RdmaDeliverService::Layout{
        .chunk_size = resources_->receive_memory().chunk_size(),
        .chunk_count = NBIO::RDMA::RdmaConnector::kReceiveChunks,
    };
}

void ReplicationService::record(const Command& command) {
    // Nothing is built when there is nobody to send it to. A replica applies
    // commands and bytes are the only form the wire has, but encoding one for
    // nobody is a RESP array, a vector of strings and a string, on every write
    // this server applies -- which is a write with no replica behind it paying
    // for a replica that is not there. Most servers in the world are this case.
    if (replicas_.empty()) [[likely]] {
        return;
    }

    // Encoded once and copied to each replica's buffer: a replica applies
    // commands, and bytes are the only form the wire has. Nothing waits on a
    // replica here -- what has not been sent is simply still in the buffer. The
    // offset moves with what is recorded and not with what is applied, so it is
    // the position of the buffer rather than a count of everything this server
    // has ever written; with no replica attached there is no buffer, and it does
    // not move.
    const std::string bytes = EncodeCommand(command);
    offset_ += bytes.size();

    for (const auto& replica : replicas_) {
        if (replica->ended) {
            continue;
        }
        replica->pending += bytes;
        // A replica that has not asked for the stream yet is not waiting on
        // anything: its buffer fills, and the writer that starts after the
        // second PSYNC finds what is already there. Waking one would be a
        // notification nobody is parked on.
        if (replica->streaming) {
            replica->writable.notify_one();
        }
    }
}

void ReplicationService::detach(const std::shared_ptr<Replica>& replica) noexcept {
    std::erase(replicas_, replica);
    replica->ended = true;
}

void ReplicationService::prune() {
    // A replica is only referred to by this list for as long as the coroutine
    // serving it runs, and its chunks only go back to the pools once it is gone.
    std::erase_if(replicas_, [](const std::shared_ptr<Replica>& replica) { return replica.use_count() == 1; });
}

NBIO::Async::Task<NBIO::Runtime, void> ReplicationService::serve() {
    if (!is_master() || resources_ == nullptr) {
        co_return;
    }

    try {
        // The device, its regions and its pools belong to the service, not to
        // this frame: every connection is built from them and every link outlives
        // the accept loop. They were opened when the service was constructed.
        acceptor_.emplace(*resources_);
        const auto bound =
            acceptor_->listen(NBIO::Net::SocketAddress::from_v4(options_.listen_address, options_.listen_port));
        if (!bound) [[unlikely]] {
            spdlog::error("replication: the listener stopped: {}", bound.error());
            co_return;
        }
        spdlog::info("replication: serving replicas on rdma://{}:{} as replid {}", options_.listen_address,
                     options_.listen_port, replid_);

        NBIO::RDMA::RdmaAcceptChannel channel(*acceptor_, NBIO::Runtime::multiplexer(), NBIO::Runtime::scheduler());
        while (true) {
            auto session = co_await channel.accept();
            if (!session) [[unlikely]] {
                spdlog::error("replication: the listener stopped: {}", session.error());
                co_return;
            }
            prune();
            NBIO::spawn(serve_replica(std::move(*session)));
        }
    } catch (const std::exception& error) {
        spdlog::error("replication: the listener stopped: {}", error.what());
    }
}

NBIO::Async::Task<NBIO::Runtime, void> ReplicationService::serve_replica(std::shared_ptr<NBIO::RDMA::RdmaSessionService> session) {
    auto replica =
        std::make_shared<Replica>(session, std::make_shared<NBIO::RDMA::RdmaDeliverService>(session, link_layout()));
    // Held for the whole connection: what record() appends to is this replica's
    // buffer, and the buffer has to be there before the snapshot is captured.
    Attachment attachment;

    try {
        if (auto ready = co_await replica->link->handshake(); !ready) [[unlikely]]
        {
            spdlog::warn("replication: a replica's handshake failed: {}", ready.error());
            co_return;
        }
        replica->link->start();

        if (!co_await send_snapshot(replica, attachment)) {
            spdlog::warn("replication: a replica went away before it had its snapshot");
            co_return;
        }

        // The replica has loaded the snapshot and asked for the stream, so from
        // here it is the master that talks: every write applied after this lands
        // in the buffer, and the pump hands the buffer over as fast as the link
        // will take it.
        replica->streaming = true;
        spdlog::info("replication: a replica is following from offset {}; {} link(s) served", offset_,
                     replicas_.size());
        co_await pump(replica);
        spdlog::info("replication: replica link closed at offset {}", offset_);
    } catch (const std::exception& error) {
        spdlog::warn("replication: the replica link failed: {}", error.what());
    }

    replica->ended = true;
    replica->writable.notify_one();
}

NBIO::Async::Task<NBIO::Runtime, bool> ReplicationService::send_snapshot(std::shared_ptr<Replica> replica,
                                                               Attachment& attachment) {
    // One lazily grown buffer for the whole handshake. It is what the requests
    // are read through and what the replies are read past, and the two share it
    // so that bytes arriving behind a line are already where the next read looks.
    NBIO::Utility::Buffer buffer(1U << 14U, 1U << 22U);
    LinkStream stream(*replica->link, buffer);

    const auto request = co_await stream.command();
    if (!request || request->empty() || request->front() != "PSYNC") [[unlikely]] {
        spdlog::warn("replication: a replica opened with something other than PSYNC");
        co_return false;
    }
    // The opening request names no replication id, so it can only be answered
    // with a snapshot. The reply to the second PSYNC is what says a replica has
    // come back to where it was.
    if (request->size() > 2 && request->at(1) != "?") [[unlikely]] {
        spdlog::warn("replication: a replica asked to continue from '{}' without having been sent a snapshot",
                     request->at(1));
        co_return false;
    }

    // The snapshot goes first and the buffer is attached the instant it has been
    // captured, which is what makes the two adjacent: the capture copies the
    // store before host_.snapshot() returns, so a write lands either in the image
    // or in what follows it, and the offset below is the position that divides
    // them.
    auto pending = host_.snapshot();
    attachment.begin(*this, replica);
    const std::uint64_t taken_at = offset_;

    if (!co_await std::move(pending)) [[unlikely]] {
        spdlog::warn("replication: this master could not write a snapshot");
        co_return false;
    }

    const auto file = host_.snapshot_file();
    if (file.empty()) [[unlikely]] {
        spdlog::warn("replication: this master has no snapshot file to serve");
        co_return false;
    }
    const std::uint64_t size = FileBytes(file);
    if (size == 0) [[unlikely]] {
        spdlog::warn("replication: the snapshot file '{}' holds nothing", file.string());
        co_return false;
    }

    std::string greeting = FullResync(replid_, taken_at) + BulkHeader(size);
    if (auto sent = co_await replica->link->send(greeting); !sent) [[unlikely]]
    {
        spdlog::warn("replication: sending the FULLRESYNC greeting failed: {}", sent.error());
        co_return false;
    }

    {
        std::ifstream source(file, std::ios::binary);
        if (!source) [[unlikely]] {
            spdlog::warn("replication: cannot read '{}'", file.string());
            co_return false;
        }
        std::vector<char> block(kSnapshotBlockBytes);
        std::uint64_t sent_bytes = 0;
        while (sent_bytes < size) {
            source.read(block.data(), static_cast<std::streamsize>(block.size()));
            const auto got = source.gcount();
            if (got <= 0) [[unlikely]] {
                spdlog::warn("replication: '{}' ended after {} of {} bytes", file.string(), sent_bytes, size);
                co_return false;
            }
            // The span points into this frame, so it is the frame -- not a copy
            // of it -- that has to stay put across the await, and it does.
            if (auto sent =
                    co_await replica->link->send(std::span<const char>(block.data(), static_cast<std::size_t>(got)));
                !sent) [[unlikely]]
            {
                spdlog::warn("replication: sending '{}' failed after {} bytes: {}", file.string(), sent_bytes,
                             sent.error());
                co_return false;
            }
            sent_bytes += static_cast<std::uint64_t>(got);
        }
    }

    // The snapshot is on its way, and what the replica does with it -- validating
    // the CRC-64 it ends with, and replacing the store -- takes time it spends
    // not reading. What it sends when it is done is the second PSYNC.
    const auto resumed = co_await stream.command();
    if (!resumed || resumed->size() < 2 || resumed->front() != "PSYNC") [[unlikely]] {
        spdlog::warn("replication: a replica did not come back after its snapshot");
        co_return false;
    }
    if (resumed->at(1) != std::to_string(replid_)) [[unlikely]] {
        // It quotes a stream this master does not have -- most likely one it had
        // before a restart -- and it cannot be continued from here.
        spdlog::warn("replication: a replica quoted replication id '{}', and this master is '{}'", resumed->at(1),
                     replid_);
        co_return false;
    }

    std::string acknowledgement = Continue(replid_);
    if (auto sent = co_await replica->link->send(acknowledgement); !sent) [[unlikely]]
    {
        spdlog::warn("replication: sending the CONTINUE failed: {}", sent.error());
        co_return false;
    }

    spdlog::info("replication: sent '{}' ({} bytes) at offset {}; {} write(s) buffered meanwhile", file.string(), size,
                 taken_at, replica->pending.size());
    co_return true;
}

NBIO::Async::Task<NBIO::Runtime, void> ReplicationService::pump(std::shared_ptr<Replica> replica) {
    while (true) {
        if (replica->pending.empty()) {
            co_await replica->writable.wait([&replica] { return !replica->pending.empty() || replica->ended; });
            if (replica->pending.empty()) {
                // Ended with nothing left to send.
                co_return;
            }
        }

        // The whole buffer is handed over at once, which is also what bounds
        // what this holds: send() cuts it into packets and waits for the window,
        // so a link that is slower than the writes leaves the rest of them where
        // they are -- in the buffer that record() keeps filling.
        std::string handover = std::move(replica->pending);
        replica->pending.clear();
        if (auto sent = co_await replica->link->send(handover); !sent) [[unlikely]]
        {
            spdlog::warn("replication: sending {} bytes of writes failed: {}", handover.size(), sent.error());
            co_return;
        }
    }
}

bool ReplicationService::slave_of(const NBIO::Net::SocketAddress& master) {
    bool expected = false;
    if (!following_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false;
    }
    if (resources_ == nullptr) {
        following_.store(false, std::memory_order_release);
        spdlog::error("replication: cannot follow {}: this instance was started without an RDMA device",
                      Endpoint(master));
        return false;
    }

    spdlog::info("replication: following master {} as a replica", Endpoint(master));
    NBIO::spawn(follow_forever(master));
    return true;
}

NBIO::Async::Task<NBIO::Runtime, void> ReplicationService::follow_forever(NBIO::Net::SocketAddress master) {
    const std::string endpoint = Endpoint(master);

    while (is_replica()) {
        bool linked = false;
        try {
            linked = co_await sync_once(master);
        } catch (const std::exception& error) {
            spdlog::warn("replication: the link to {} failed: {}", endpoint, error.what());
        }

        if (linked) {
            host_.link_changed(false);
            spdlog::warn("replication: the link to {} ended", endpoint);
        }

        // The session borrows the connector and the connector borrows the
        // device, so the whole link goes before another one is built -- and a
        // successful connect hands its communication id to the session, which is
        // why one cannot be reused.
        link_.reset();
        co_await NBIO::Time::SystemTimeService{}.sleep(std::chrono::seconds(1));
    }
    co_return;
}

NBIO::Async::Task<NBIO::Runtime, bool> ReplicationService::sync_once(NBIO::Net::SocketAddress master) {
    link_ = std::make_unique<ReplicaLink>(resources_);
    NBIO::RDMA::RdmaConnectChannel channel(link_->connector, NBIO::Runtime::multiplexer(), NBIO::Runtime::scheduler());
    auto session = co_await channel.connect(master);
    if (!session) [[unlikely]] {
        spdlog::warn("replication: connecting to {} failed: {}", Endpoint(master), session.error());
        co_return false;
    }

    link_->link = std::make_shared<NBIO::RDMA::RdmaDeliverService>(*session, link_layout());
    if (auto ready = co_await link_->link->handshake(); !ready) [[unlikely]]
    {
        spdlog::warn("replication: the handshake with {} failed: {}", Endpoint(master), ready.error());
        co_return false;
    }
    link_->link->start();

    NBIO::Utility::Buffer buffer(1U << 14U, 1U << 22U);
    LinkStream stream(*link_->link, buffer);

    // The opening request: no id and no offset, which is the only thing this end
    // can honestly say before it has seen the master's stream. It is answered
    // with a snapshot and the position that snapshot belongs to.
    if (auto posted = co_await link_->link->send(EncodePSYNC("?", "-1")); !posted) [[unlikely]]
    {
        spdlog::warn("replication: asking {} to synchronise failed: {}", Endpoint(master), posted.error());
        co_return false;
    }

    auto greeting = co_await stream.line();
    if (!greeting) [[unlikely]] {
        spdlog::warn("replication: {} stopped before it answered the PSYNC", Endpoint(master));
        co_return false;
    }
    // +FULLRESYNC <replid> <offset>
    if (!greeting->starts_with("+FULLRESYNC ")) [[unlikely]] {
        spdlog::warn("replication: {} answered '{}' where a FULLRESYNC was owed", Endpoint(master), *greeting);
        co_return false;
    }
    std::string_view rest(*greeting);
    rest.remove_prefix(std::string_view("+FULLRESYNC ").size());
    const auto space = rest.find(' ');
    std::uint64_t replid = 0;
    std::uint64_t offset = 0;
    if (space == std::string_view::npos || !ParseUnsigned(rest.substr(0, space), replid) ||
        !ParseUnsigned(rest.substr(space + 1), offset)) [[unlikely]] {
        spdlog::warn("replication: {} sent a FULLRESYNC this end cannot read: '{}'", Endpoint(master), *greeting);
        co_return false;
    }

    auto header = co_await stream.line();
    std::uint64_t bytes = 0;
    if (!header || header->empty() || header->front() != '$' ||
        !ParseUnsigned(std::string_view(*header).substr(1), bytes)) [[unlikely]] {
        spdlog::warn("replication: {} did not say how long its snapshot is", Endpoint(master));
        co_return false;
    }

    spdlog::info("replication: master {} is replid {} at offset {}; receiving {} bytes of snapshot", Endpoint(master),
                 replid, offset, bytes);

    // The snapshot lands beside the RDB path and is moved onto it only once
    // every byte has arrived, so a transfer that dies half way leaves nothing
    // that looks like a snapshot.
    const auto file = host_.snapshot_file();
    if (file.empty()) [[unlikely]] {
        spdlog::warn("replication: this server has no RDB file to receive a snapshot into");
        co_return false;
    }
    const auto incoming = std::filesystem::path(file).concat(".incoming");

    {
        std::ofstream sink(incoming, std::ios::binary | std::ios::trunc);
        if (!sink) [[unlikely]] {
            spdlog::warn("replication: cannot write '{}'", incoming.string());
            co_return false;
        }
        const bool whole = co_await stream.take(bytes, [&sink](std::span<const char> piece) {
            sink.write(piece.data(), static_cast<std::streamsize>(piece.size()));
            return static_cast<bool>(sink);
        });
        if (!whole) [[unlikely]] {
            spdlog::warn("replication: the snapshot from {} ended early", Endpoint(master));
            std::error_code ignored;
            std::filesystem::remove(incoming, ignored);
            co_return false;
        }
    }

    std::error_code moved;
    std::filesystem::rename(incoming, file, moved);
    if (moved) [[unlikely]] {
        spdlog::warn("replication: cannot put the snapshot at '{}': {}", file.string(), moved.message());
        co_return false;
    }

    // Loading is what verifies: an RDB ends with a CRC-64 over its own bytes and
    // a load refuses one whose checksum does not match, so a transfer that lost
    // or damaged a byte cannot become the store.
    if (!host_.restore(file)) [[unlikely]] {
        spdlog::warn("replication: '{}' did not validate against its own CRC and was not replayed", file.string());
        co_return false;
    }
    offset_ = offset;
    spdlog::info("replication: replayed '{}', which is the master's log up to offset {}", file.string(), offset_);

    // Loaded, so the stream may start: the master holds everything applied since
    // the snapshot, and this is what asks for it. The offset is where this end
    // is, which is where the snapshot left it.
    if (auto posted = co_await link_->link->send(EncodePSYNC(std::to_string(replid), "0")); !posted) [[unlikely]]
    {
        spdlog::warn("replication: asking {} for the stream failed: {}", Endpoint(master), posted.error());
        co_return false;
    }
    auto acknowledgement = co_await stream.line();
    if (!acknowledgement) [[unlikely]] {
        spdlog::warn("replication: {} stopped before it acknowledged the snapshot", Endpoint(master));
        co_return false;
    }

    host_.link_changed(true);
    spdlog::info("replication: following master {} from offset {}", Endpoint(master), offset_);

    // From here the master talks and this end applies. The decode is kept across
    // payloads because a payload ends wherever the master cut it, which is as
    // likely to be the middle of a command as between two, and the decoder is
    // what holds the half of it that has arrived.
    auto decoder = RESP::Decode(buffer);
    while (true) {
        while (decoder.poll() == RESP::DecodeStatus::kComplete) {
            const auto& decoded = decoder.result();
            const CommandValidation validation =
                decoded.object ? ValidateCommand(*decoded.object) : CommandValidation{};
            if (!validation || !host_.apply(*validation.command)) [[unlikely]] {
                spdlog::warn("replication: '{}' could not be applied",
                             decoded.error.empty() ? validation.error : decoded.error);
                co_return true;
            }
            // The bytes of a command are what the master's log holds for it, so
            // counting them is what keeps this end's offset the same number the
            // master's is.
            offset_ += EncodeCommand(*validation.command).size();
            decoder = RESP::Decode(buffer);
        }
        if (!co_await stream.more()) [[unlikely]] {
            co_return true;
        }
    }
}
}  // namespace KV

#endif  // defined(__linux__)
