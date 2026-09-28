#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/RdmaHeader.hpp>
#include <Foundation/NBIO/ConditionVariable.hpp>
#include <Foundation/NBIO/RdmaSessionService.hpp>

#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>

namespace Foundation::NBIO
{
// Bytes over an RDMA session, which on its own is a bag of fixed-size chunks.
//
// The session stays what it is -- take a chunk, hand it to the device, take one back
// -- and this is the layer that gives those chunks a shape and a flow: it frames what
// it sends as RdmaPackets, numbers them, tells the peer what it has finished with, and
// holds its own sends inside what the peer said it could take. Above it, a payload is
// just bytes, so the replication link can put RESP in it and stop worrying about
// packets.
//
// It owns the session, because a link with acknowledgements on it cannot be driven
// from both ends at once: an ack arrives while a sender is waiting for room, and only
// one coroutine may own the receive channel. So the reader is this service's own --
// it takes packets off the session, files the acks into the window and puts the
// payloads on a queue -- and everything a caller does goes through send(), receive()
// and release().
//
// The one rule worth knowing before reading the code: an acknowledgement means the
// buffers are *back*, not that the bytes were seen. A receiver that acked on arrival
// would let a sender refill a queue whose chunks its consumer has not released, and
// the receive pool would run dry under the peer's sends -- which is the queue pair
// being torn down with RNR_RETRY_EXC_ERR, the failure this whole arrangement exists to
// avoid. So an ack is sent when a payload is released, and what it acknowledges is the
// highest packet whose payload has been released.
class RdmaDeliverService final : public std::enable_shared_from_this<RdmaDeliverService>
{
  public:
    // What this end can take. The chunk size is the manager's, header included, so the
    // payload a packet carries is that less the header; the count is how many chunks
    // the receive pool holds, which is therefore also how many packets the peer may
    // have in flight at once.
    struct Layout
    {
        std::uint64_t chunk_size{0};
        std::uint64_t chunk_count{0};

        // What one packet's payload can be, which is what a caller's payload is cut
        // at on the way out.
        std::uint64_t payload_size() const noexcept
        {
            return chunk_size > sizeof(Foundation::Core::RdmaPacket)
                       ? chunk_size - sizeof(Foundation::Core::RdmaPacket)
                       : 0;
        }
    };

    // The session has to be established already: the handshake is a packet, and a
    // packet needs a connection.
    RdmaDeliverService(std::shared_ptr<RdmaSessionService> session, Layout layout);
    ~RdmaDeliverService() noexcept;

    RdmaDeliverService(const RdmaDeliverService &) = delete;
    RdmaDeliverService &operator=(const RdmaDeliverService &) = delete;
    RdmaDeliverService(RdmaDeliverService &&) = delete;
    RdmaDeliverService &operator=(RdmaDeliverService &&) = delete;

    // Says what this end can take and waits to hear the same from the peer. Both sides
    // do this as soon as they are connected, so neither has to know who speaks first,
    // and nothing else is sent or received until it has happened.
    Foundation::NBIO::Task<Core::expected<void, std::string>> handshake();

    // Starts the reader. Until this is called nothing takes packets off the session,
    // so nothing is acknowledged and nothing arrives. Called once.
    void start();

    // Ends the link from this side: the reader is cancelled and whatever is waiting for
    // a packet is told that none is coming. The reader is a root coroutine, and a live
    // root keeps a thread's run() from returning -- so this is also what lets a stopped
    // service be destroyed, since the reader's own frame holds a reference to it.
    void stop() noexcept;

    // One payload, cut into as many packets as it needs. Waits for the peer to
    // acknowledge enough that the packets this adds stay inside the window, which is
    // the backpressure: a consumer that is slow simply does not get more.
    Foundation::NBIO::Task<Core::expected<void, std::string>> send(std::span<const char> payload);

    // The next payload the peer sent, empty once the link is over.
    Foundation::NBIO::Task<Core::expected<std::optional<std::span<char>>, std::string>> receive();

    // Gives a payload back, which puts its chunk back in the pool -- and that is what
    // lets the peer send more, so the acknowledgement goes out here rather than when
    // the bytes arrived.
    Foundation::NBIO::Task<Core::expected<void, std::string>> release(std::span<char> payload);

    // What the peer has acknowledged, and what this end has sent: the distance between
    // them is what has to stay inside the peer's chunk count.
    std::uint64_t acknowledged() const noexcept
    {
        return peer_acknowledged_;
    }

    std::uint64_t sent() const noexcept
    {
        return sent_;
    }

    std::uint64_t taken() const noexcept
    {
        return taken_;
    }

  private:
    // Everything one packet's worth: the header it arrived with, and the payload
    // inside it.
    struct Incoming
    {
        std::span<char> packet{};
        std::uint64_t sequence{0};
        Foundation::Core::RdmaPacketType type{};
        std::uint64_t acknowledge{0};
    };

    // A payload off the session and not yet asked for, with the number of the packet
    // it came in: the number is what an acknowledgement ends up naming, and it can only
    // be known when the chunk it arrived in goes back.
    struct Held
    {
        std::span<char> payload{};
        std::uint64_t sequence{0};
    };

    // Reads one packet off the session and says what was in it. The packet is not
    // released: its chunk goes back when its payload does.
    Foundation::NBIO::Task<Core::expected<Incoming, std::string>> read_packet();
    // Works out what a packet meant: the ack inside it widens what may be sent, and a
    // payload goes on the queue for whoever asks for one.
    Foundation::NBIO::Task<Core::expected<void, std::string>> absorb(Incoming packet);

    // One packet carrying `payload`, waited for: the header, the sequence, whatever
    // acknowledgement is owed, and the type the caller says it is. A meta packet is the
    // one thing sent through here that is not a payload, and its numbers mean the same
    // thing they mean on every other packet.
    Foundation::NBIO::Task<Core::expected<void, std::string>> send_packet(std::span<const char> payload, Foundation::Core::RdmaPacketType type);

    // Waits until the packets already in flight leave room for `packets` more.
    Foundation::NBIO::Task<Core::expected<void, std::string>> wait_for_room(std::uint64_t packets);

    bool room_for(std::uint64_t packets) const noexcept;

    // What start() runs: the reader's own loop, taking packets off the session until it
    // fails or the service is stopped. A static member rather than a lambda, so that the
    // service arrives as an ordinary parameter -- a coroutine's body is the wrong place
    // to be reading a closure.
    static Foundation::NBIO::Task<void> Read(std::shared_ptr<RdmaDeliverService> self);

    // What start() spawned the reader as, so that stop() can cancel it. Cancelling is
    // what lets the scheduler destroy the frame, and destroying the frame is what
    // releases the reference that frame holds to this service.
    Foundation::Async::CoroutineToken reader_{};

    std::shared_ptr<RdmaSessionService> session_;
    Layout mine_;
    Layout peer_{};
    bool handshaken_{false};
    bool ended_{false};

    // This end's own numbering, and the two counters that decide whether it may send:
    // what it has sent, and what the peer has said it finished with.
    std::uint64_t sent_{0};
    std::uint64_t peer_acknowledged_{0};
    // What this end has taken, and what has been released: the second is what an ack
    // carries, because the second is what says the buffers are back.
    std::uint64_t taken_{0};
    std::uint64_t released_{0};

    // Payloads taken off the session and not yet asked for. Each one is holding a chunk
    // out of the receive pool until it is released.
    std::deque<Held> ready_;
    // What the peer has been told, so that an acknowledgement is only sent when it says
    // something new.
    std::uint64_t acknowledged_to_peer_{0};
    Foundation::NBIO::ConditionVariable room_;
    Foundation::NBIO::ConditionVariable ready_available_;
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
