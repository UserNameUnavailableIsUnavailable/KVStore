#if defined(__linux__)

#include "RdmaTransfer.hpp"

#include <Foundation/Core/Byte.hpp>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <cstring>
#include <fstream>
#include <system_error>

namespace KV
{
namespace
{
namespace NBIO = Foundation::NBIO;

// A count this large cannot be a payload, so it is refused before a byte of it
// is written: 1 TiB is well past any snapshot a replica is asked to hold.
constexpr std::uint64_t kMaxPayloadBytes = 1ULL << 40;

std::span<const char> AsBytes(std::string_view text) noexcept
{
    return {text.data(), text.size()};
}

// Why a link stopped carrying messages, for the log. No error at all means the
// peer is simply gone.
std::string LinkState(NBIO::RdmaSession &session)
{
    const auto &stream = session.connection();
    if (stream.failed())
    {
        return stream.error();
    }
    if (stream.peer_closed())
    {
        return "the peer closed the connection";
    }
    return "no error reported";
}

// Wire numbers go out in network byte order, as they do in every protocol since
// TCP's own header and in the snapshot this payload usually is: the high byte of
// a length is where a length's high byte belongs, rather than wherever the
// machine that happens to be reading puts its most significant byte first. The
// conversion is Foundation/Core/Byte.hpp's, which is a no-op on a big-endian
// machine and a swap anywhere else.
void PutU32(char *out, std::uint32_t value) noexcept
{
    const auto ordered = Foundation::Core::to_big_endian(value);
    std::memcpy(out, &ordered, sizeof(ordered));
}

void PutU64(char *out, std::uint64_t value) noexcept
{
    const auto ordered = Foundation::Core::to_big_endian(value);
    std::memcpy(out, &ordered, sizeof(ordered));
}

std::uint32_t GetU32(const char *in) noexcept
{
    std::uint32_t ordered = 0;
    std::memcpy(&ordered, in, sizeof(ordered));
    return Foundation::Core::from_big_endian(ordered);
}

std::uint64_t GetU64(const char *in) noexcept
{
    std::uint64_t ordered = 0;
    std::memcpy(&ordered, in, sizeof(ordered));
    return Foundation::Core::from_big_endian(ordered);
}

// What one attempt at a control message ended with. Finding no chunk to put it
// in is a status of its own rather than a failure: every chunk is in flight, and
// the caller comes back once a completion has retired one.
enum class SendOutcome
{
    kSent,
    kNoChunk,
    kFailed,
};

// One message: a chunk off the stream, filled and handed to the device, with
// nothing waited for. The completion that hands the chunk back is a later
// concern, and reporting it is what poll_send is for. `error` is filled in
// whenever the answer is kFailed.
SendOutcome SendMessage(NBIO::RdmaSession &session, std::span<const char> message, std::string &error)
{
    auto acquired = session.send_channel().acquire();
    if (!acquired) [[unlikely]]
    {
        error = acquired.error();
        return SendOutcome::kFailed;
    }
    if (!*acquired) [[unlikely]]
    {
        return SendOutcome::kNoChunk;
    }

    const std::span<char> chunk = **acquired;
    if (message.size() > chunk.size()) [[unlikely]]
    {
        error = "the message does not fit a send chunk";
        return SendOutcome::kFailed;
    }

    std::memcpy(chunk.data(), message.data(), message.size());
    if (const auto sent = session.send(chunk, message.size()); !sent) [[unlikely]]
    {
        error = sent.error();
        return SendOutcome::kFailed;
    }
    return SendOutcome::kSent;
}

// A message that is part of the conversation rather than the payload: waited for
// a chunk to put it in, because losing one would stall the other end.
NBIO::Task<bool> SendControl(NBIO::RdmaSession &session, std::span<const char> message)
{
    while (true)
    {
        std::string error;
        const auto outcome = SendMessage(session, message, error);
        if (outcome == SendOutcome::kSent)
        {
            co_return true;
        }
        if (outcome == SendOutcome::kFailed) [[unlikely]]
        {
            spdlog::warn("transfer: sending a control message failed: {}", error);
            co_return false;
        }

        // Every chunk is in flight; one of them has to come back first.
        spdlog::debug("transfer: no send chunk free, waiting for a completion");
        const auto reaped = co_await session.poll_send(1);
        if (!reaped) [[unlikely]]
        {
            spdlog::warn("transfer: the link stopped reporting completions: {}", reaped.error());
            co_return false;
        }
        if (*reaped == 0 && session.send_channel().outstanding() != 0) [[unlikely]]
        {
            spdlog::warn("transfer: the link stopped reporting completions");
            co_return false;
        }
    }
}

// What the receiver has said about the payload: how many packets it has written,
// and -- in place of the credit for the last one -- how many bytes it wrote,
// which is the transfer's whole answer.
struct Progress
{
    std::uint64_t written{0};
    std::optional<std::uint64_t> confirmed{};
};

// Folds a credit into the count, which only ever moves forward. The credit
// carries the number written rather than a fresh allowance, so the two ends
// cannot drift apart by counting differently and a report arriving out of order
// cannot walk the count back. The count is always short of the whole payload,
// because the report for the last packet is the confirmation.
bool FoldCredit(std::uint32_t reported, std::uint64_t packets, std::uint64_t &written)
{
    if (reported == 0 || reported >= packets) [[unlikely]]
    {
        spdlog::warn("transfer: the receiver credited {} of {} packets", reported, packets);
        return false;
    }
    written = std::max<std::uint64_t>(written, reported);
    return true;
}

enum class ReportStatus
{
    kTaken,
    kEmpty,
    kFailed,
};

// Takes in one report of the receiver's progress, waiting for it only when
// `wait`. An empty queue is a status of its own rather than a failure in the
// non-waiting case, which is what lets the sender take in what is already there
// without parking on nothing.
NBIO::Task<ReportStatus> ReadReport(NBIO::RdmaSession &session, std::uint64_t packets, bool wait, Progress &progress)
{
    auto received = co_await (wait ? session.receive() : session.try_receive());
    if (!received) [[unlikely]]
    {
        spdlog::warn("transfer: the link ended before the receiver reported progress ({})", received.error());
        co_return ReportStatus::kFailed;
    }
    std::optional<std::span<char>> message = std::move(*received);
    if (!message)
    {
        if (wait)
        {
            spdlog::warn("transfer: the link ended before the receiver reported progress ({})", LinkState(session));
            co_return ReportStatus::kFailed;
        }
        co_return ReportStatus::kEmpty;
    }

    const auto size = message->size();
    const std::string_view view(message->data(), size);
    std::uint32_t credited = 0;
    std::uint64_t confirmed = 0;
    const bool is_credit = DecodeCreditHeader(view, credited);
    const bool is_confirmation = !is_credit && DecodeDoneHeader(view, confirmed);
    if (const auto released = session.release(*message); !released) [[unlikely]]
    {
        spdlog::warn("transfer: handing a report back failed: {}", released.error());
        co_return ReportStatus::kFailed;
    }
    if (!is_credit && !is_confirmation) [[unlikely]]
    {
        spdlog::warn("transfer: expected a report and got {} bytes", size);
        co_return ReportStatus::kFailed;
    }

    if (is_confirmation)
    {
        progress.confirmed = confirmed;
    }
    else if (!FoldCredit(credited, packets, progress.written))
    {
        co_return ReportStatus::kFailed;
    }
    co_return ReportStatus::kTaken;
}

// Waits for the next report and folds it in, answering false when the link ended
// before one arrived.
NBIO::Task<bool> WaitForReport(NBIO::RdmaSession &session, std::uint64_t packets, Progress &progress)
{
    co_return co_await ReadReport(session, packets, true, progress) == ReportStatus::kTaken;
}

// Takes in every report that has already arrived, so a window that has been
// opened up is used in one pass rather than one packet per wake-up. Answers
// false when the link ended; an empty queue is the case it is here for.
NBIO::Task<bool> HarvestReports(NBIO::RdmaSession &session, std::uint64_t packets, Progress &progress)
{
    while (true)
    {
        const auto status = co_await ReadReport(session, packets, false, progress);
        if (status == ReportStatus::kFailed)
        {
            co_return false;
        }
        if (status == ReportStatus::kEmpty || progress.confirmed)
        {
            co_return true;
        }
    }
}

// Drops a half-written file. The failure is already being reported, so failing
// again while cleaning up after it is not worth another one.
void RemoveQuietly(const std::filesystem::path &path) noexcept
{
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}
} // namespace

std::string_view WireWord(PayloadKind kind) noexcept
{
    return kind == PayloadKind::kCommands ? kRESPHeader : kRDBHeader;
}

std::size_t OpenMessageBytes(PayloadKind kind) noexcept
{
    // An opening for commands carries where the run should start as well, because
    // the receiver is the one that knows where it got to; a snapshot is taken
    // whole, so there is no offset for it to name.
    const std::size_t offset_bytes = kind == PayloadKind::kCommands ? sizeof(std::uint64_t) : 0;
    return WireWord(kind).size() + 1 + sizeof(std::uint32_t) * 2 + offset_bytes;
}

TransferOffer ReceiverOffer(PayloadKind kind, std::uint32_t chunk_size, std::uint64_t offset) noexcept
{
    // The window is what this end has receives posted for: every packet the
    // sender may have in flight has to land in one of them, and the credits are
    // what hold it to that. The chunk size is the caller's, because it is the
    // size of the buffers this end posted, the kind is the caller's because it is
    // what this end will do with the bytes once they are here, and the offset is
    // where a run of commands is to start.
    return {.kind = kind,
            .chunk_size = chunk_size,
            .num_chunks = Foundation::Core::RdmaConnector::kReceiveChunks,
            .offset = offset};
}

std::uint32_t NegotiatedWindow(const TransferOffer &offer) noexcept
{
    // The window is what this end has receives posted for, and it is also the
    // number of messages the receiver may have in flight toward this end: it
    // reports once per packet and the last packet's report is the confirmation,
    // so `window` packets can produce at most `window` reports. Those two counts
    // are the same number because the window bounds the span the receiver has to
    // account for, and a packet in flight and the report that will pay for it are
    // the same slot in the accounting -- the credits arriving and the packets
    // arriving are, between them, never more than the window.
    //
    // So nothing has to be held back, and nothing may be exceeded: a report that
    // arrives with no receive posted is what ends an iWARP connection.
    constexpr std::uint32_t sender_receives = Foundation::Core::RdmaConnector::kReceiveChunks;
    return std::min(offer.num_chunks, sender_receives);
}

namespace
{
// Reads an operation word and answers where the fields behind it start.
std::optional<std::size_t> MatchOperation(std::string_view message, std::string_view operation) noexcept
{
    if (message.size() < operation.size() + 1 || message[operation.size()] != '\0' ||
        message.substr(0, operation.size()) != operation)
    {
        return std::nullopt;
    }
    return operation.size() + 1;
}
} // namespace

std::string EncodeOpen(const TransferOffer &offer)
{
    const auto word = WireWord(offer.kind);
    std::string message(OpenMessageBytes(offer.kind), '\0');
    std::memcpy(message.data(), word.data(), word.size());
    const auto fields = word.size() + 1;
    PutU32(message.data() + fields, offer.chunk_size);
    PutU32(message.data() + fields + sizeof(std::uint32_t), offer.num_chunks);
    if (offer.kind == PayloadKind::kCommands)
    {
        PutU64(message.data() + fields + sizeof(std::uint32_t) * 2, offer.offset);
    }
    return message;
}

bool DecodeOpen(std::string_view message, TransferOffer &offer)
{
    // Either kind may be what opened a transfer, so both words are tried: the one
    // that matches is also the one that says where the fields behind it start.
    // A word that is neither is refused rather than read as one of them, and the
    // sizes are different, so a message cannot be half of one and half of the
    // other.
    for (const auto kind : {PayloadKind::kSnapshot, PayloadKind::kCommands})
    {
        const auto word = WireWord(kind);
        const auto fields = MatchOperation(message, word);
        if (!fields || message.size() != OpenMessageBytes(kind))
        {
            continue;
        }
        offer.kind = kind;
        offer.chunk_size = GetU32(message.data() + *fields);
        offer.num_chunks = GetU32(message.data() + *fields + sizeof(std::uint32_t));
        offer.offset = kind == PayloadKind::kCommands ? GetU64(message.data() + *fields + sizeof(std::uint32_t) * 2) : 0;
        return true;
    }
    return false;
}

std::string EncodePlanHeader(std::uint32_t chunk_size, std::uint32_t packets, std::uint64_t offset)
{
    std::string message(kPlanMessageBytes, '\0');
    std::memcpy(message.data(), kPlanHeader.data(), kPlanHeader.size());
    const auto fields = kPlanHeader.size() + 1;
    PutU32(message.data() + fields, chunk_size);
    PutU32(message.data() + fields + sizeof(std::uint32_t), packets);
    PutU64(message.data() + fields + sizeof(std::uint32_t) * 2, offset);
    return message;
}

bool DecodePlanHeader(std::string_view message, std::uint32_t &chunk_size, std::uint32_t &packets, std::uint64_t &offset)
{
    const auto fields = MatchOperation(message, kPlanHeader);
    if (!fields || message.size() != kPlanMessageBytes)
    {
        return false;
    }
    chunk_size = GetU32(message.data() + *fields);
    packets = GetU32(message.data() + *fields + sizeof(std::uint32_t));
    offset = GetU64(message.data() + *fields + sizeof(std::uint32_t) * 2);
    return true;
}

std::string EncodeCreditHeader(std::uint32_t written)
{
    std::string message(kCreditMessageBytes, '\0');
    std::memcpy(message.data(), kCreditHeader.data(), kCreditHeader.size());
    PutU32(message.data() + kCreditHeader.size() + 1, written);
    return message;
}

bool DecodeCreditHeader(std::string_view message, std::uint32_t &written)
{
    const auto fields = MatchOperation(message, kCreditHeader);
    if (!fields || message.size() != kCreditMessageBytes)
    {
        return false;
    }
    written = GetU32(message.data() + *fields);
    return true;
}

std::string EncodeDoneHeader(std::uint64_t bytes)
{
    std::string message(kDoneMessageBytes, '\0');
    std::memcpy(message.data(), kDoneHeader.data(), kDoneHeader.size());
    PutU64(message.data() + kDoneHeader.size() + 1, bytes);
    return message;
}

bool DecodeDoneHeader(std::string_view message, std::uint64_t &bytes)
{
    const auto fields = MatchOperation(message, kDoneHeader);
    if (!fields || message.size() != kDoneMessageBytes)
    {
        return false;
    }
    bytes = GetU64(message.data() + *fields);
    return true;
}

std::uint64_t PacketCount(std::uintmax_t payload_size, std::size_t packet_size) noexcept
{
    if (payload_size == 0 || packet_size == 0)
    {
        return 0;
    }
    return (static_cast<std::uint64_t>(payload_size) + packet_size - 1) / packet_size;
}

Foundation::NBIO::Task<std::optional<TransferOffer>> AcceptTransfer(NBIO::RdmaSession &session)
{
    auto received = co_await session.receive();
    if (!received) [[unlikely]]
    {
        spdlog::warn("transfer: the link ended before a receiver opened a transfer ({})", received.error());
        co_return std::nullopt;
    }
    std::optional<std::span<char>> opening = std::move(*received);
    if (!opening)
    {
        co_return std::nullopt;
    }

    const auto size = opening->size();
    TransferOffer offer;
    const bool decoded = DecodeOpen(std::string_view(opening->data(), size), offer);
    if (const auto released = session.release(*opening); !released) [[unlikely]]
    {
        spdlog::warn("transfer: handing an opening back failed: {}", released.error());
        co_return std::nullopt;
    }
    if (!decoded) [[unlikely]]
    {
        spdlog::warn("transfer: expected an opening ({} or {}) and got {} bytes", kRDBHeader, kRESPHeader, size);
        co_return std::nullopt;
    }
    if (offer.chunk_size == 0 || offer.num_chunks == 0) [[unlikely]]
    {
        spdlog::warn("transfer: the receiver offered nothing to send into");
        co_return std::nullopt;
    }
    spdlog::debug("transfer: a receiver opened a {} transfer", WireWord(offer.kind));

    co_return offer;
}

namespace
{
// The packet size both ends can live with: the most the receiver will accept,
// and the most this end's own chunks can hold. Sending more than the receiver
// offered would not be refused, it would be *truncated*, so the smaller of the
// two is the only safe answer.
std::uint32_t AgreedChunkSize(const TransferOffer &offer, std::size_t packet_size) noexcept
{
    const auto mine = static_cast<std::uint64_t>(packet_size);
    const auto agreed = std::min<std::uint64_t>(mine, offer.chunk_size);
    return static_cast<std::uint32_t>(std::min<std::uint64_t>(agreed, std::numeric_limits<std::uint32_t>::max()));
}
} // namespace

Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendPayload(NBIO::RdmaSession &session,
                                                                 const TransferOffer &offer, PayloadKind sending,
                                                                 std::uint64_t end_offset, std::uintmax_t size,
                                                                 const PacketReader &read, std::size_t packet_size)
{
    // What this end decides: how big a packet both ends can carry, and how many
    // of them the payload is. The receiver is told, because the decision is the
    // sender's and only the sender knows what it is sending -- but the one thing
    // this end does not decide is what the bytes *are*: the receiver opened the
    // transfer for one kind, and sending it the other would put bytes on the wire
    // that the far end has already said it cannot use.
    if (offer.kind != sending) [[unlikely]]
    {
        spdlog::warn("transfer: the receiver opened for {} and this end has {} to send", WireWord(offer.kind),
                     WireWord(sending));
        co_return std::nullopt;
    }

    const auto chunk_size = AgreedChunkSize(offer, packet_size);
    const auto window = NegotiatedWindow(offer);
    if (chunk_size == 0 || window == 0) [[unlikely]]
    {
        spdlog::warn("transfer: nothing to send with: {} byte packets and a window of {}", chunk_size, window);
        co_return std::nullopt;
    }

    // The count goes on the wire in 32 bits, and a credit carries the count
    // written out in the same width, so a payload whose packets do not fit it
    // could be announced but never reported one by one. Nothing real comes near
    // the bound: a tebibyte of payload at a kilobyte a packet is 2^30.
    const auto count = PacketCount(size, chunk_size);
    if (count == 0) [[unlikely]]
    {
        spdlog::warn("transfer: there is nothing to send");
        co_return std::nullopt;
    }
    if (count > std::numeric_limits<std::uint32_t>::max()) [[unlikely]]
    {
        spdlog::warn("transfer: {} packets is more than a transfer can count", count);
        co_return std::nullopt;
    }
    const auto packets = static_cast<std::uint32_t>(count);
    spdlog::info("transfer: sending {} bytes as {} packets of {} bytes, window of {}", size, packets, chunk_size, window);

    const std::string plan_message = EncodePlanHeader(chunk_size, packets, end_offset);
    if (!co_await SendControl(session, AsBytes(plan_message))) [[unlikely]]
    {
        co_return std::nullopt;
    }

    // `written` is what the receiver has reported writing and `sent` is the
    // packets handed to the device, so `sent - written` is the span the receiver
    // has still to account for. It is never allowed to reach the window.
    Progress progress;
    std::uint64_t sent = 0;
    std::uintmax_t offset = 0;
    while (sent < packets)
    {
        // Take in every report already waiting first, so a window that has been
        // opened up is used in one pass instead of one packet per wake-up.
        if (!co_await HarvestReports(session, packets, progress))
        {
            co_return std::nullopt;
        }
        if (progress.confirmed) [[unlikely]]
        {
            // It cannot have written them all before this end has sent them
            // all, so this is a peer confirming a payload it never received.
            spdlog::warn("transfer: the receiver confirmed the payload after {} of {} packets", sent, packets);
            co_return std::nullopt;
        }

        if (sent >= progress.written + window)
        {
            // The window is full: the receiver has as many packets to account
            // for as it said it could hold, and nothing may be sent until it
            // writes one out.
            spdlog::debug("transfer: window full at {} packets, waiting for the receiver", sent);
            if (!co_await WaitForReport(session, packets, progress))
            {
                co_return std::nullopt;
            }
            continue;
        }

        auto acquired = session.send_channel().acquire();
        if (!acquired) [[unlikely]]
        {
            spdlog::warn("transfer: the send channel failed: {}", acquired.error());
            co_return std::nullopt;
        }
        if (!*acquired) [[unlikely]]
        {
            // Every chunk is in flight: one has to come back before another
            // packet can be built. This is the sender's own limit, and it is a
            // tighter one than the window when the pool is smaller.
            const auto reaped = co_await session.poll_send(1);
            if (!reaped) [[unlikely]]
            {
                spdlog::warn("transfer: the link stopped reporting completions after {} bytes: {}", offset,
                             reaped.error());
                co_return std::nullopt;
            }
            if (*reaped == 0 && session.send_channel().outstanding() != 0) [[unlikely]]
            {
                spdlog::warn("transfer: the link stopped reporting completions after {} bytes", offset);
                co_return std::nullopt;
            }
            continue;
        }

        const std::span<char> chunk = **acquired;
        const auto wanted = std::min<std::size_t>(chunk_size, static_cast<std::size_t>(size - offset));
        const auto filled = read(chunk.first(wanted));
        if (filled != wanted) [[unlikely]]
        {
            spdlog::warn("transfer: the payload gave {} bytes where {} were announced", filled, wanted);
            co_return std::nullopt;
        }

        const auto posted = session.send(chunk, filled);
        if (!posted) [[unlikely]]
        {
            spdlog::warn("transfer: sending byte {} failed: {}", offset, posted.error());
            co_return std::nullopt;
        }
        offset += filled;
        ++sent;
        spdlog::debug("transfer: packet {} of {} sent, {} in the receiver's hands", sent, packets,
                      sent - progress.written);
    }

    // The confirmation stands in for the credit for the last packet, so it is the
    // last report of the transfer and everything the receiver wrote before it is
    // already here. Reading it is what says the payload arrived rather than
    // merely left -- the one thing a send completion cannot say.
    while (!progress.confirmed)
    {
        if (!co_await WaitForReport(session, packets, progress))
        {
            spdlog::warn("transfer: the receiver stopped reporting after {} of {} packets", progress.written, packets);
            co_return std::nullopt;
        }
    }
    if (progress.written != packets - 1) [[unlikely]]
    {
        // One report per packet, so a count short of that means a report that
        // should have been there was not, and the window was being measured
        // against something the receiver never said.
        spdlog::warn("transfer: the receiver confirmed after {} of {} credits", progress.written, packets - 1);
        co_return std::nullopt;
    }
    if (*progress.confirmed != size) [[unlikely]]
    {
        spdlog::warn("transfer: the receiver confirmed {} of {} bytes", *progress.confirmed, size);
        co_return std::nullopt;
    }

    co_return offset;
}

Foundation::NBIO::Task<std::optional<TransferResult>> ReceivePayload(NBIO::RdmaSession &session, PayloadKind kind,
                                                                    std::uint64_t offset, const PacketWriter &write,
                                                                    std::size_t chunk_size)
{
    if (chunk_size < kPlanMessageBytes || chunk_size > std::numeric_limits<std::uint32_t>::max()) [[unlikely]]
    {
        spdlog::warn("transfer: a chunk of {} bytes cannot carry the transfer's own messages", chunk_size);
        co_return std::nullopt;
    }

    // The offer, and with it what this end will do with the bytes, its own chunk
    // size and its receive window. It goes before anything is read, because it is
    // what tells the sender it may start.
    const TransferOffer offer = ReceiverOffer(kind, static_cast<std::uint32_t>(chunk_size), offset);
    const std::string opening = EncodeOpen(offer);
    if (!co_await SendControl(session, AsBytes(opening))) [[unlikely]]
    {
        co_return std::nullopt;
    }
    spdlog::info("transfer: opened a {} transfer, offering {} byte chunks, {} of them at once", WireWord(kind),
                 offer.chunk_size, offer.num_chunks);

    // What the sender decided to do with the offer.
    auto received_plan = co_await session.receive();
    if (!received_plan) [[unlikely]]
    {
        spdlog::warn("transfer: the link ended before the sender said what it would send ({})",
                     received_plan.error());
        co_return std::nullopt;
    }
    std::optional<std::span<char>> plan_message = std::move(*received_plan);
    if (!plan_message)
    {
        spdlog::warn("transfer: the link ended before the sender said what it would send ({})", LinkState(session));
        co_return std::nullopt;
    }

    const auto plan_size = plan_message->size();
    std::uint32_t planned_chunk = 0;
    std::uint32_t packets = 0;
    std::uint64_t end_offset = 0;
    const bool decoded =
        DecodePlanHeader(std::string_view(plan_message->data(), plan_size), planned_chunk, packets, end_offset);
    if (const auto released = session.release(*plan_message); !released) [[unlikely]]
    {
        spdlog::warn("transfer: handing the plan back failed: {}", released.error());
        co_return std::nullopt;
    }
    if (!decoded || packets == 0) [[unlikely]]
    {
        spdlog::warn("transfer: the sender announced no payload");
        co_return std::nullopt;
    }
    if (planned_chunk == 0 || planned_chunk > offer.chunk_size) [[unlikely]]
    {
        // More than was offered does not fit the receives this end posted, and
        // the device truncates what does not fit rather than refusing it.
        spdlog::warn("transfer: the sender decided on {} byte packets where {} were offered", planned_chunk,
                     offer.chunk_size);
        co_return std::nullopt;
    }
    if (packets > kMaxPayloadBytes / planned_chunk) [[unlikely]]
    {
        spdlog::warn("transfer: the sender announced {} packets, more than this end will accept", packets);
        co_return std::nullopt;
    }
    spdlog::info("transfer: receiving {} packets of at most {} bytes", packets, planned_chunk);

    std::uintmax_t written_bytes = 0;
    std::uint32_t written = 0;
    for (std::uint32_t index = 0; index < packets; ++index)
    {
        auto received = co_await session.receive();
        if (!received) [[unlikely]]
        {
            spdlog::warn("transfer: the link ended after {} of {} packets ({})", index, packets, received.error());
            co_return std::nullopt;
        }
        std::optional<std::span<char>> packet = std::move(*received);
        if (!packet) [[unlikely]]
        {
            spdlog::warn("transfer: the link ended after {} of {} packets ({})", index, packets, LinkState(session));
            co_return std::nullopt;
        }

        const auto length = packet->size();
        if (length > planned_chunk) [[unlikely]]
        {
            spdlog::warn("transfer: a packet of {} bytes is larger than the agreed {} bytes", length, planned_chunk);
            // The link is already being given up on, so a failure while handing
            // the buffer back is not worth a second report.
            (void)session.release(*packet);
            co_return std::nullopt;
        }

        const bool taken = write(*packet);
        // Handed back *before* the credit goes out, so a credit is always the
        // proof that this end has the buffer back to receive the next one into.
        // Reposting it is also what keeps a receive waiting for the packet that
        // is already on its way.
        if (const auto released = session.release(*packet); !released) [[unlikely]]
        {
            spdlog::warn("transfer: handing a packet back failed: {}", released.error());
            co_return std::nullopt;
        }
        if (!taken) [[unlikely]]
        {
            spdlog::warn("transfer: packet {} could not be written out", index);
            co_return std::nullopt;
        }
        written_bytes += static_cast<std::uintmax_t>(length);
        ++written;

        // One report per packet: a credit while packets remain, carrying the
        // count written out rather than a fresh allowance, so the count cannot
        // drift the way two independently incremented allowances can and the
        // sender can see exactly how far the payload has got. It also means
        // every packet the sender has in the receiver's hands is matched by a
        // receive that is back, because the report goes after the release.
        //
        // The last packet is left out: the confirmation says what a credit for
        // it would have said -- every packet is written -- and then adds the
        // byte total, so a credit of its own would be the same message twice.
        // Leaving it out is also what lets the window be the receiver's whole
        // receive count, since the sender then has to hold no more reports than
        // it is holding packets.
        if (index + 1 < packets)
        {
            const std::string credit = EncodeCreditHeader(written);
            if (!co_await SendControl(session, AsBytes(credit))) [[unlikely]]
            {
                co_return std::nullopt;
            }
            spdlog::debug("transfer: packet {} of {} written, credited", index + 1, packets);
        }
    }

    // The confirmation, and with it the answer the sender is waiting for: the
    // payload is written, not merely sent.
    const std::string done = EncodeDoneHeader(written_bytes);
    spdlog::debug("transfer: {} packets written, confirming {} bytes", packets, written_bytes);
    if (!co_await SendControl(session, AsBytes(done))) [[unlikely]]
    {
        co_return std::nullopt;
    }
    spdlog::debug("transfer: the confirmation is posted");

    // A posted send is not a sent one, and this is the last thing this end says:
    // whoever called this may drop the session the moment it returns, so let the
    // confirmation leave the device first. It also has to be acknowledged, or a
    // provider will not finish destroying the queue it completed on.
    if (const auto settled = co_await session.poll_send(0); !settled) [[unlikely]]
    {
        spdlog::warn("transfer: the confirmation was never reported as sent: {}", settled.error());
    }
    spdlog::debug("transfer: the confirmation has left the device");

    co_return TransferResult{.bytes = written_bytes, .offset = end_offset};
}

Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendFile(NBIO::RdmaSession &session,
                                                               const std::filesystem::path &path,
                                                               std::size_t packet_size)
{
    const auto offer = co_await AcceptTransfer(session);
    if (!offer)
    {
        co_return std::nullopt;
    }
    co_return co_await SendFile(session, *offer, path, packet_size);
}

Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendFile(NBIO::RdmaSession &session, const TransferOffer &offer,
                                                               const std::filesystem::path &path,
                                                               std::size_t packet_size, std::uint64_t offset)
{
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error) [[unlikely]]
    {
        spdlog::warn("transfer: cannot size '{}': {}", path.string(), error.message());
        co_return std::nullopt;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file) [[unlikely]]
    {
        spdlog::warn("transfer: cannot open '{}'", path.string());
        co_return std::nullopt;
    }

    const PacketReader read = [&file](std::span<char> packet) -> std::size_t {
        file.read(packet.data(), static_cast<std::streamsize>(packet.size()));
        return static_cast<std::size_t>(file.gcount());
    };

    co_return co_await SendPayload(session, offer, PayloadKind::kSnapshot, offset, size, read, packet_size);
}

Foundation::NBIO::Task<std::optional<TransferResult>> ReceiveFile(NBIO::RdmaSession &session,
                                                                  const std::filesystem::path &path,
                                                                  std::size_t chunk_size)
{
    // Written beside the target and moved onto it at the end, so a transfer that
    // dies half way leaves nothing that looks like a whole file.
    const std::filesystem::path partial = path.string() + ".part";
    std::ofstream file(partial, std::ios::binary | std::ios::trunc);
    if (!file) [[unlikely]]
    {
        spdlog::warn("transfer: cannot write '{}'", partial.string());
        co_return std::nullopt;
    }

    const PacketWriter write = [&file](std::span<const char> packet) -> bool {
        file.write(packet.data(), static_cast<std::streamsize>(packet.size()));
        return static_cast<bool>(file);
    };

    const auto received = co_await ReceivePayload(session, PayloadKind::kSnapshot, 0, write, chunk_size);
    file.close();
    if (!received) [[unlikely]]
    {
        RemoveQuietly(partial);
        co_return std::nullopt;
    }

    std::error_code error;
    std::filesystem::rename(partial, path, error);
    if (error) [[unlikely]]
    {
        spdlog::warn("transfer: cannot move '{}' onto '{}': {}", partial.string(), path.string(), error.message());
        RemoveQuietly(partial);
        co_return std::nullopt;
    }

    co_return received;
}

Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendCommands(NBIO::RdmaSession &session,
                                                                   const TransferOffer &offer,
                                                                   std::uint64_t end_offset, std::uintmax_t size,
                                                                   const PacketReader &read,
                                                                   std::size_t packet_size)
{
    co_return co_await SendPayload(session, offer, PayloadKind::kCommands, end_offset, size, read, packet_size);
}

Foundation::NBIO::Task<std::optional<TransferResult>> ReceiveCommands(NBIO::RdmaSession &session,
                                                                      std::uint64_t offset,
                                                                      const PacketWriter &write,
                                                                      std::size_t chunk_size)
{
    co_return co_await ReceivePayload(session, PayloadKind::kCommands, offset, write, chunk_size);
}
} // namespace KV

#endif // defined(__linux__)
