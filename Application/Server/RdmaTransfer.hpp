#pragma once
#if defined(__linux__)

#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/NBIO/RdmaSessionService.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace KV
{
// Moving a payload from one server to another over an RDMA session.
//
// The wire is four messages. Each one starts with its operation as a
// NUL-terminated word, so a message that does not belong to a transfer is
// refused instead of read as the wrong kind of number, and every number is in
// network byte order -- big endian -- so the high byte of a count is where a
// reader looks for it rather than where the machine it landed on would put it.
//
//   receiver -> sender : <kind>\0 chunk_size:u32 num_chunks:u32 [offset:u64]
//   sender  -> receiver: "PLAN\0" chunk_size:u32 packets:u32 offset:u64
//   sender  -> receiver: the payload, one packet per message
//   receiver -> sender : "CREDIT\0" written:u32    after each packet but the last
//   receiver -> sender : "DONE\0" bytes:u64        in place of the last credit
//
// The offset is the log the payload belongs to, and it is the one number the two
// ends agree about beyond the framing. An opening for commands carries where the
// receiver wants the run to start -- which is also its acknowledgement of
// everything before it -- and the plan says where the payload *ends*: for a
// snapshot, the log position the image was taken at, which is where a replica
// follows from; for commands, the position of the byte after the last one sent,
// from which the receiver subtracts whatever it could not apply.
//
// The opening's word is the payload's *kind*, and it is the one thing beyond the
// framing that the two ends have to agree on: "RDB" for an opaque payload the
// receiver writes out whole, "RESP" for RESP-encoded commands it decodes as they
// arrive. The transfer never looks inside either, so the kind is what stands in
// for looking: the receiver knows which of the two it is holding, and a sender
// that has the other one is refused rather than allowed to put bytes on the wire
// that the far end cannot use.
//
// Every packet is reported exactly once: a credit for each packet the receiver
// writes, and the confirmation in place of the credit for the last one. The two
// would otherwise be the same message twice -- a credit for the last packet
// already says every packet is written -- so the confirmation carries the byte
// total instead and is the only message that says the payload arrived rather
// than merely left.
//
// The receiver opens, because it is the one that knows it has receives posted;
// the sender never has to guess whether the other end is ready. What it offers
// is what it can take:
//
//   chunk_size  the largest packet it will accept. More than this does not fit
//               the receives it has posted, and a message that does not fit is
//               *truncated* by the device rather than refused, so this bound has
//               to be respected rather than discovered.
//   num_chunks  how many packets it can hold at once.
//
// The sender then decides what it will actually do, because the two ends need
// not be configured the same and both limits are hard:
//
//   chunk_size  min(the offer, the sender's own chunk size)
//   window      min(the offer's num_chunks, the sender's posted receives)
//
// Neither half of that minimum is about the payload alone: messages come back
// the other way, and they land in the *sender's* receive queue. What makes the
// two counts the same number is that the reports mirror the packets one for one:
//
//   unread packets in the receiver  +  unread reports in the sender
//       = packets received - reports read
//       <= packets sent - credits read
//       <= window
//
// so a single window bounds what the two queues hold between them, and neither
// can reach past it however the traffic divides. Nothing is held back for the
// confirmation, because it is not an extra message: it is the report for the
// last packet, arriving where a credit would have. It is not merely that no
// receive is reserved -- none may be exceeded: a report that arrives with no
// receive posted is what ends a connection on iWARP.
//
// Flow control is measured in packets the receiver has written out, never in
// packets the sender has handed to the device. A send completion only says the
// bytes left the sender: it says nothing about the peer having had a receive to
// put them in, and on iWARP there is no RNR to fall back on. A sender that
// refilled its window from its own completions therefore runs the peer out of
// receives; that is not hypothetical, it is how a burst into a 16-deep receive
// queue died at packet 17.
//
// So the sender holds a count of what the receiver has taken:
//
//   written   the highest cumulative packet count the receiver has reported, 0
//             before the first credit. It is cumulative rather than "one more",
//             so the two ends cannot drift apart by counting differently, and
//             the sender can see exactly how far the payload has really got. It
//             is always short of the packet total, because the report for the
//             last packet is the confirmation.
//   sent      how many packets this end has handed to the device.
//
// and it may send another packet only while
//
//   sent < written + window
//
// i.e. while the packets the receiver has not written out yet are fewer than the
// window. In the terms of the sender's own bookkeeping that is
//
//   in flight <= min(num_chunks the receiver posted, credits it has left)
//
// with the credits starting at the window and one being spent per packet. The
// sender's own chunk pool bounds it further, because it cannot post more sends
// than it has chunks: a window wider than the pool simply means the pool is what
// is binding.
//
// The receiver grants a credit as it writes each packet out, so a credit is the
// proof that its buffer is back, and it releases the buffer *before* it sends
// the credit, so there is never a moment where the sender is entitled to a slot
// the receiver has not reposted. The payload is whatever the sender can hand out
// a packet at a time: a file, a buffer in memory, or nothing at all.

// The header each message starts with. The opening's header is the payload's
// kind rather than an operation of its own; the rest follow it so that a message
// is what it says it is even when it arrives where none was expected.
inline constexpr std::string_view kRDBHeader = "RDB";
inline constexpr std::string_view kRESPHeader = "RESP";
inline constexpr std::string_view kPlanHeader = "PLAN";
inline constexpr std::string_view kCreditHeader = "CREDIT";
inline constexpr std::string_view kDoneHeader = "DONE";

inline constexpr std::size_t kPlanMessageBytes = kPlanHeader.size() + 1 + sizeof(std::uint32_t) * 2 + sizeof(std::uint64_t);
inline constexpr std::size_t kCreditMessageBytes = kCreditHeader.size() + 1 + sizeof(std::uint32_t);
inline constexpr std::size_t kDoneMessageBytes = kDoneHeader.size() + 1 + sizeof(std::uint64_t);

// What the bytes of a transfer are, which is the one thing the framing cannot
// say for itself: an opaque payload the receiver writes out whole -- the
// snapshot of a store -- or RESP-encoded commands it decodes as they arrive.
// Both are bytes to the transfer, so the kind is not a difference in the
// plumbing but in what the far end does with what the plumbing delivers.
enum class PayloadKind : std::uint8_t
{
    kSnapshot,
    kCommands,
};

// The word that names a kind on the wire, and the size of an opening that names
// it. The two words are different lengths, so an opening is one of two sizes and
// never something in between.
std::string_view WireWord(PayloadKind kind) noexcept;
std::size_t OpenMessageBytes(PayloadKind kind) noexcept;

// What a receiver offers when it opens a transfer: what the payload is, the
// largest packet it will accept, how many packets it can hold at once, and -- for
// commands -- the log offset it wants the run to start at.
struct TransferOffer
{
    PayloadKind kind{PayloadKind::kSnapshot};
    std::uint32_t chunk_size{0};
    std::uint32_t num_chunks{0};
    std::uint64_t offset{0};
};

// What this end can offer. The kind is what it will do with the bytes and the
// chunk size is the size of the buffers it posted, so both are the caller's to
// pass in; the count is what it has receives posted for. The offset is where the
// receiver wants a run of commands to start, and it means nothing to a snapshot,
// which is taken whole.
TransferOffer ReceiverOffer(PayloadKind kind, std::uint32_t chunk_size, std::uint64_t offset = 0) noexcept;

// What the sender decided to do with an offer, which is what the payload is cut
// at and how the window is measured from then on. The offset is where the log the
// payload belongs to has got to after it: for a snapshot that is the position it
// was taken at, for commands the position of the first byte not sent.
struct TransferPlan
{
    std::uint32_t chunk_size{0};
    std::uint32_t packets{0};
    std::uint32_t window{0};
    std::uint64_t offset{0};
};

// The two ends of the negotiation: what the receiver offers, and what the sender
// makes of it.
std::string EncodeOpen(const TransferOffer &offer);
bool DecodeOpen(std::string_view message, TransferOffer &offer);

// `packets` is how many packets the payload is cut into. Zero means "nothing to
// send", which is not a payload any snapshot can have. The count is 32 bits,
// which is all the wire needs and all a credit can carry: a payload whose
// packets did not fit that width could be announced but never reported, so the
// sender refuses it instead of announcing it. `offset` is where the log the
// payload belongs to has got to after it.
std::string EncodePlanHeader(std::uint32_t chunk_size, std::uint32_t packets, std::uint64_t offset);
bool DecodePlanHeader(std::string_view message, std::uint32_t &chunk_size, std::uint32_t &packets, std::uint64_t &offset);

// How many packets the receiver has written out so far, counting from the start
// of the payload. Never the whole payload: the report for the last packet is the
// confirmation, which is where the count would otherwise reach the total.
std::string EncodeCreditHeader(std::uint32_t written);
bool DecodeCreditHeader(std::string_view message, std::uint32_t &written);

// How many bytes the receiver wrote out, sent in place of the credit for the
// last packet. A credit for it would say only what this says and less, so this
// is the message that closes a transfer: it is the only one that tells the
// sender the payload did not just leave but arrive.
std::string EncodeDoneHeader(std::uint64_t bytes);
bool DecodeDoneHeader(std::string_view message, std::uint64_t &bytes);

// How many packets a payload of this size is cut into at this size.
std::uint64_t PacketCount(std::uintmax_t payload_size, std::size_t chunk_size) noexcept;

// The window the sender will use for an offer: what the receiver can hold, and
// what the sender's own receive queue can hold reports in. The two counts are
// the same number because the reports mirror the packets one for one, so one
// window bounds both queues together.
std::uint32_t NegotiatedWindow(const TransferOffer &offer) noexcept;

// Fills one packet, answering how many bytes it put there. Every packet but the
// last has to be filled whole: the packet count the receiver is given is what
// says how many are coming, and a short one would put the two ends out of step.
using PacketReader = std::function<std::size_t(std::span<char> packet)>;
// Takes one packet, answering whether it is written out. A credit is only sent
// once it says so, so a slow writer slows the transfer down rather than losing
// anything.
using PacketWriter = std::function<bool(std::span<const char> packet)>;

// What a receive ended with: how many bytes arrived, and the log offset the
// payload belongs to after it -- the snapshot's position, or where the next batch
// of commands starts from.
struct TransferResult
{
    std::uintmax_t bytes{0};
    std::uint64_t offset{0};
};

// Waits for a receiver to open a transfer and answers what it offered. Nothing
// when what arrived was not an opening.
Foundation::NBIO::Task<std::optional<TransferOffer>> AcceptTransfer(Foundation::NBIO::RdmaSessionService &session);

// Sends `size` bytes over an already open transfer, one packet per message, and
// never more than the window's worth unacknowledged. `sending` is what this end
// is about to put on the wire and it has to be what the receiver opened the
// transfer for: a snapshot is not something a receiver that asked for commands
// can use, and the other way round neither. `end_offset` is where the log this
// payload belongs to will have got to once it has arrived, which is what tells
// the receiver where to follow from. `packet_size` is this end's own chunk size,
// which is the most a packet can be. Answers how many bytes the receiver says it
// wrote out.
Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendPayload(Foundation::NBIO::RdmaSessionService &session,
                                                                 const TransferOffer &offer, PayloadKind sending,
                                                                 std::uint64_t end_offset, std::uintmax_t size,
                                                                 const PacketReader &read, std::size_t packet_size);

// Opens a transfer for `kind` and hands every packet to `write`. `chunk_size` is
// this end's own, and it is what the offer is made of; it is also a promise about
// the largest packet this end can take, since a packet that does not fit the
// receive it lands in is truncated rather than refused. `offset` is where it
// wants a run of commands to start, and answers with where the payload left the
// log.
Foundation::NBIO::Task<std::optional<TransferResult>> ReceivePayload(Foundation::NBIO::RdmaSessionService &session,
                                                                     PayloadKind kind, std::uint64_t offset,
                                                                     const PacketWriter &write,
                                                                     std::size_t chunk_size);

// The two ends over a file. Both cut it at the negotiated size, which is the
// size of one message.
Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendFile(Foundation::NBIO::RdmaSessionService &session,
                                                               const std::filesystem::path &path,
                                                               std::size_t packet_size);

// The same, over a transfer a receiver has already opened. A sender that has to
// do something between the two -- take the snapshot it is about to send, say --
// waits for the opening itself and then calls this. `offset` is the log position
// the image is taken at, which is where a replica follows from.
Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendFile(Foundation::NBIO::RdmaSessionService &session,
                                                              const TransferOffer &offer,
                                                              const std::filesystem::path &path,
                                                              std::size_t packet_size, std::uint64_t offset = 0);

// Receives a file and puts it at `path`. Answers how many bytes were written and
// the log position the image belongs to, or nothing when the transfer did not
// complete, in which case `path` is left as it was: the bytes land beside it and
// are moved onto it only once all of them are here.
Foundation::NBIO::Task<std::optional<TransferResult>> ReceiveFile(Foundation::NBIO::RdmaSessionService &session,
                                                                 const std::filesystem::path &path,
                                                                 std::size_t chunk_size);

// The same, over the other kind: RESP-encoded commands, which a replica applies
// rather than writes down. The bytes are a run of commands, so a packet that ends
// in the middle of one is the normal case rather than an error, and the `write`
// that takes them is what decodes and applies them. `end_offset` is where the run
// leaves the master's log.
Foundation::NBIO::Task<std::optional<std::uintmax_t>> SendCommands(Foundation::NBIO::RdmaSessionService &session,
                                                                  const TransferOffer &offer,
                                                                  std::uint64_t end_offset, std::uintmax_t size,
                                                                  const PacketReader &read,
                                                                  std::size_t packet_size);

// Asks for the commands the master has applied from `offset` onwards, handing
// every packet to `write`.
Foundation::NBIO::Task<std::optional<TransferResult>> ReceiveCommands(Foundation::NBIO::RdmaSessionService &session,
                                                                     std::uint64_t offset,
                                                                     const PacketWriter &write,
                                                                     std::size_t chunk_size);
} // namespace KV

#endif // defined(__linux__)
