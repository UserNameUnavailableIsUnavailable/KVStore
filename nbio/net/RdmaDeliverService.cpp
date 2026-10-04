#include "RdmaDeliverService.hpp"

#if defined(__linux__)

#include <spdlog/spdlog.h>

#include <nbio/nbio.hpp>
#include <nbio/utility/Byte.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <algorithm>
#include <array>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace nbio::net {
namespace {

// Every packet carries the acknowledgement it knows, so this only decides how often a
// receiver that is sending nothing of its own says something: a quarter of the window is
// often enough that a sender never runs out of room, and rare enough that the
// acknowledgements are not the traffic.
std::uint64_t AckEvery(std::uint64_t chunk_count) noexcept { return std::max<std::uint64_t>(1, chunk_count / 4); }

// The layout, on the wire as two eight-byte numbers with everything to be learned from
// it: a meta packet is the one thing sent before either end knows what the other can
// hold, so it has to be small.
constexpr std::size_t kMetaBytes = 2 * sizeof(std::uint64_t);

std::array<char, kMetaBytes> EncodeLayout(const RdmaDeliverService::Layout& layout) noexcept {
    std::array<char, kMetaBytes> bytes{};
    const auto size = nbio::utility::ToBigEndian(layout.chunk_size);
    const auto count = nbio::utility::ToBigEndian(layout.chunk_count);
    std::memcpy(bytes.data(), &size, sizeof(size));
    std::memcpy(bytes.data() + sizeof(size), &count, sizeof(count));
    return bytes;
}

bool DecodeLayout(std::span<const char> bytes, RdmaDeliverService::Layout& layout) noexcept {
    if (bytes.size() < kMetaBytes) {
        return false;
    }
    std::uint64_t size = 0;
    std::uint64_t count = 0;
    std::memcpy(&size, bytes.data(), sizeof(size));
    std::memcpy(&count, bytes.data() + sizeof(size), sizeof(count));
    layout.chunk_size = nbio::utility::FromBigEndian(size);
    layout.chunk_count = nbio::utility::FromBigEndian(count);
    return layout.payload_size() > 0 && layout.chunk_count > 0;
}

// The header, read off the front of a chunk. The packet is the chunk: what the caller
// is handed later is what comes after this.
const nbio::net::RdmaHeader& AsPacket(std::span<char> chunk) noexcept {
    return *reinterpret_cast<const nbio::net::RdmaHeader*>(chunk.data());
}

// The chunk a payload came in, which is the only way back to it: the payload was cut
// from it and nothing else records where the header started.
std::span<char> ChunkOf(std::span<char> payload, const RdmaDeliverService::Layout& layout) noexcept {
    return std::span<char>(payload.data() - sizeof(nbio::net::RdmaHeader), static_cast<std::size_t>(layout.chunk_size));
}
}  // namespace

RdmaDeliverService::RdmaDeliverService(std::shared_ptr<RdmaSessionService> session, Layout layout)
    : session_(std::move(session)), mine_(layout) {
    if (session_ == nullptr) {
        throw std::invalid_argument("an rdma deliver service needs a session");
    }
    if (mine_.payload_size() == 0 || mine_.chunk_count == 0) [[unlikely]] {
        throw std::invalid_argument("an rdma deliver service needs chunks that hold a packet and a payload");
    }
    // What the peer may have in flight is what this end has actually posted receives for,
    // and that is the connection's own depth rather than the pool's size: a pool of
    // thousands says nothing about how many of its chunks the device has been handed. A
    // window wider than the posted receives is one the peer will fill and then fail on,
    // with the queue pair torn down for RNR_RETRY_EXC_ERR -- so it is refused here rather
    // than discovered there.
    if (mine_.chunk_count > nbio::net::RdmaConnector::kReceiveChunks) [[unlikely]] {
        throw std::invalid_argument("the layout offers " + std::to_string(mine_.chunk_count) +
                                    " chunks where the connection posts receives for only " +
                                    std::to_string(nbio::net::RdmaConnector::kReceiveChunks));
    }
}

RdmaDeliverService::~RdmaDeliverService() noexcept = default;

nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> RdmaDeliverService::handshake() {
    // Both ends say what they can take as soon as they are connected, so neither has to
    // know who goes first. The reader is not running yet, so the peer's own meta is
    // read here rather than by it.
    const auto mine = EncodeLayout(mine_);
    const auto open = nbio::net::RdmaPacketType::kMeta | nbio::net::RdmaPacketType::kAck;
    if (auto sent = co_await SendPacket(std::span<const char>(mine.data(), mine.size()), open); !sent) [[unlikely]]
    {
        co_return nbio::utility::unexpected(sent.error());
    }

    auto incoming = co_await ReadPacket();
    if (!incoming) [[unlikely]] {
        co_return nbio::utility::unexpected(incoming.error());
    }
    if (!(incoming->type & nbio::net::RdmaPacketType::kMeta)) [[unlikely]] {
        co_return nbio::utility::unexpected(std::string{"the peer opened with something other than what it can take"});
    }
    if (!DecodeLayout(incoming->packet.subspan(sizeof(nbio::net::RdmaHeader)), peer_)) [[unlikely]] {
        co_return nbio::utility::unexpected(std::string{"the peer said it can take nothing"});
    }
    // A meta packet carries no payload, so its chunk is finished with as soon as it has
    // been read.
    if (auto released = session_->Release(incoming->packet); !released) [[unlikely]]
    {
        co_return nbio::utility::unexpected(released.error());
    }
    taken_ = 1;
    // The peer's meta was its first packet, so it is also the first thing this end has
    // finished with: releasing it is what puts that chunk back, and the acknowledgement
    // for it is already owed.
    released_ = 1;
    acknowledged_to_peer_ = 1;
    handshaken_ = true;
    co_return nbio::utility::expected<void, std::string>{};
}

void RdmaDeliverService::Start() {
    // The reader is the service's own, and it is the only thing that ever takes a chunk
    // off the session: an ack arrives while a sender is waiting for room, so the two
    // cannot be the same coroutine.
    reader_ = nbio::Spawn(Read(shared_from_this()));
}

nbio::async::Task<nbio::runtime, void> RdmaDeliverService::Read(std::shared_ptr<RdmaDeliverService> self) {
    while (!self->ended_) {
        auto packet = co_await self->ReadPacket();
        if (!packet) [[unlikely]] {
            // The link is over. Whichever coroutine is waiting is told, because nothing
            // else will ever be: an ack can never arrive now.
            self->ended_ = true;
            self->room_.NotifyAll();
            self->ready_available_.NotifyAll();
            spdlog::warn("deliver: the link ended: {}", packet.error());
            co_return;
        }
        if (auto absorbed = co_await self->Absorb(std::move(*packet)); !absorbed) [[unlikely]]
        {
            self->ended_ = true;
            self->room_.NotifyAll();
            self->ready_available_.NotifyAll();
            spdlog::warn("deliver: {}", absorbed.error());
            co_return;
        }
    }
}

void RdmaDeliverService::Stop() noexcept {
    // Cancelling is what ends the reader, and cancelling is what it takes: the reader is
    // Parked on the session's receive channel, and a packet to wake it with is exactly
    // what is not coming. The scheduler never resumes a cancelled coroutine, and
    // destroys its frame at the next iteration -- which is what lets go of the
    // shared_ptr that frame holds to this service. That is why a service nobody stops is
    // one nobody destroys, and why its reader keeps the thread's Run() from returning.
    //
    // It is the destroy-at-reclaim that makes this work, and not the token: the reader
    // is Parked in a channel this service owns, so a frame that outlived reclamation
    // would hold the service that holds the channel that holds the frame.
    ended_ = true;
    reader_.Cancel();
    room_.NotifyAll();
    ready_available_.NotifyAll();
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<RdmaDeliverService::Incoming, std::string>> RdmaDeliverService::ReadPacket() {
    auto incoming = co_await session_->Receive();
    if (!incoming) [[unlikely]] {
        co_return nbio::utility::unexpected(incoming.error());
    }
    if (!*incoming) [[unlikely]] {
        // Nothing was waiting, which the session only reports once the link is over.
        co_return nbio::utility::unexpected(std::string{"the connection is gone"});
    }

    const std::span<char> packet = **incoming;
    if (packet.size() < sizeof(nbio::net::RdmaHeader)) [[unlikely]] {
        // A completion shorter than a header is not a packet this protocol sent, and
        // guessing what it was is worse than stopping.
        (void)session_->Release(packet);
        co_return nbio::utility::unexpected(std::string{"a packet arrived without a header"});
    }

    const auto& header = AsPacket(packet);
    co_return Incoming{
        .packet = packet, .sequence = header.sequence, .type = header.type, .acknowledge = header.acknowledge};
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> RdmaDeliverService::Absorb(Incoming packet) {
    taken_ = std::max(taken_, packet.sequence);
    if (packet.type & nbio::net::RdmaPacketType::kAck) {
        // What the peer has finished with, which is what lets this end send again.
        acknowledged_ = std::max(acknowledged_, packet.acknowledge);
        room_.NotifyAll();
    }

    if (!(packet.type & nbio::net::RdmaPacketType::kPayload)) {
        // An ack carries nothing, so its chunk is finished with as soon as it has been
        // read. It is still a packet the peer sent and numbered, though, and its number
        // is released like any other: a number that was never released would hold a slot
        // in the peer's window for good, and a link that sends an ack every so often
        // would slowly fill the window with packets that have nothing in them.
        if (auto released = session_->Release(packet.packet); !released) [[unlikely]]
        {
            co_return nbio::utility::unexpected(released.error());
        }
        released_ = std::max(released_, packet.sequence);
        co_return nbio::utility::expected<void, std::string>{};
    }

    // A payload's chunk stays out of the pool until its payload is released, which is
    // what makes the acknowledgement mean "the buffer is back" rather than "the bytes
    // went past".
    ready_.push_back(Held{.payload = packet.packet.subspan(sizeof(nbio::net::RdmaHeader)), .sequence = packet.sequence});
    ready_available_.NotifyOne();
    co_return nbio::utility::expected<void, std::string>{};
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> RdmaDeliverService::Send(std::span<const char> payload) {
    if (!handshaken_) [[unlikely]] {
        co_return nbio::utility::unexpected(std::string{"the handshake has not happened"});
    }

    // What one packet can carry is the smaller of the two ends' payload sizes: this end
    // has to be able to fill a chunk, and the peer has to be able to receive what is in
    // it. Both ends know both numbers, so both cut at the same width.
    const auto width = std::min(mine_.payload_size(), peer_.payload_size());
    std::size_t offset = 0;
    while (offset < payload.size()) {
        const auto take = static_cast<std::size_t>(std::min<std::uint64_t>(width, payload.size() - offset));
        if (auto sent = co_await SendPacket(payload.subspan(offset, take),
                                             nbio::net::RdmaPacketType::kPayload | nbio::net::RdmaPacketType::kAck);
            !sent) [[unlikely]]
        {
            co_return nbio::utility::unexpected(sent.error());
        }
        offset += take;
    }
    co_return nbio::utility::expected<void, std::string>{};
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> RdmaDeliverService::SendPacket(std::span<const char> payload,
                                                                                          nbio::net::RdmaPacketType type) {
    if (auto room = co_await WaitForRoom(1); !room) [[unlikely]]
    {
        co_return nbio::utility::unexpected(room.error());
    }

    // A chunk to fill, waited for when every one of them is in flight: this is the
    // sender's own limit, and a tighter one than the window when the pool is small.
    std::span<char> chunk{};
    while (chunk.empty()) {
        auto acquired = session_->send_channel().Acquire();
        if (!acquired) [[unlikely]] {
            co_return nbio::utility::unexpected(acquired.error());
        }
        if (*acquired) {
            chunk = **acquired;
            break;
        }
        const auto reaped = co_await session_->PollSend(1);
        if (!reaped) [[unlikely]] {
            co_return nbio::utility::unexpected(reaped.error());
        }
        if (*reaped == 0 && session_->send_channel().outstanding() != 0) [[unlikely]] {
            co_return nbio::utility::unexpected(std::string{"the link stopped reporting completions"});
        }
    }

    if (payload.size() + sizeof(nbio::net::RdmaHeader) > chunk.size()) [[unlikely]] {
        co_return nbio::utility::unexpected(std::string{"a packet does not fit a chunk"});
    }

    // The header goes on here rather than at the caller's hands: what a caller writes is
    // a payload, and what the device carries is a packet.
    nbio::net::RdmaHeader header{};
    header.sequence = ++sent_;
    header.acknowledge = released_;
    header.type = type;
    std::memcpy(chunk.data(), &header, sizeof(header));
    if (!payload.empty()) {
        std::memcpy(chunk.data() + sizeof(header), payload.data(), payload.size());
    }
    acknowledged_to_peer_ = std::max(acknowledged_to_peer_, header.acknowledge);
    room_.NotifyOne();

    if (auto posted = session_->Send(chunk, sizeof(header) + payload.size()); !posted) [[unlikely]]
    {
        co_return nbio::utility::unexpected(posted.error());
    }
    co_return nbio::utility::expected<void, std::string>{};
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<std::optional<std::span<char>>, std::string>> RdmaDeliverService::Receive() {
    co_await ready_available_.wait([this] { return !ready_.empty() || ended_; });
    if (ready_.empty()) [[unlikely]] {
        co_return nbio::utility::expected<std::optional<std::span<char>>, std::string>{std::nullopt};
    }
    co_return std::optional<std::span<char>>{ready_.front().payload};
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> RdmaDeliverService::release(std::span<char> payload) {
    if (ready_.empty() || ready_.front().payload.data() != payload.data()) [[unlikely]] {
        co_return nbio::utility::unexpected(std::string{"a payload is released in the order it arrived"});
    }
    const Held held = ready_.front();
    ready_.pop_front();

    // The chunk goes back first: it is what the peer is waiting for, and what the
    // acknowledgement that follows is a claim about.
    if (auto released = session_->Release(ChunkOf(held.payload, mine_)); !released) [[unlikely]]
    {
        co_return nbio::utility::unexpected(released.error());
    }
    released_ = std::max(released_, held.sequence);

    // An acknowledgement every packet would be the traffic this protocol exists to
    // avoid, so one goes out when enough has accumulated to be worth saying -- and every
    // packet already carries it meanwhile.
    if (released_ > acknowledged_to_peer_ && released_ - acknowledged_to_peer_ >= AckEvery(mine_.chunk_count)) {
        if (auto sent = co_await SendPacket(std::span<const char>{}, nbio::net::RdmaPacketType::kAck); !sent) [[unlikely]] {
            co_return nbio::utility::unexpected(sent.error());
        }
    }
    co_return nbio::utility::expected<void, std::string>{};
}

bool RdmaDeliverService::RoomFor(std::uint64_t packets) const noexcept {
    if (!handshaken_) {
        // Before the peer has said what it can take, the only packet allowed is the one
        // that says it: the handshake is what the window is made of.
        return sent_ - acknowledged_ + packets <= 1;
    }
    return sent_ - acknowledged_ + packets <= peer_.chunk_count;
}

nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> RdmaDeliverService::WaitForRoom(std::uint64_t packets) {
    co_await room_.wait([this, packets] { return ended_ || RoomFor(packets); });
    if (ended_ && !RoomFor(packets)) [[unlikely]] {
        co_return nbio::utility::unexpected(std::string{"the link ended with the window full"});
    }
    co_return nbio::utility::expected<void, std::string>{};
}
}  // namespace nbio::net

#endif  // defined(__linux__)




