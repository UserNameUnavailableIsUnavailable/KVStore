#pragma once
#if defined(__linux__)

#include <nbio/async/Coroutine.hpp>
#include <nbio/async/Task.hpp>
#include <nbio/utility/Expected.hpp>
#include <nbio/net/RdmaHeader.hpp>
#include <nbio/notification/ConditionVariable.hpp>
#include <nbio/net/RdmaSessionService.hpp>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>

namespace nbio::net {
class RdmaDeliverService final : public std::enable_shared_from_this<RdmaDeliverService> {
   public:
    struct Layout {
        std::uint64_t chunk_size{0};
        std::uint64_t chunk_count{0};

        // What one packet's payload can be, which is what a caller's payload is cut
        // at on the way out.
        std::uint64_t payload_size() const noexcept {
            return chunk_size > sizeof(nbio::net::RdmaHeader) ? chunk_size - sizeof(nbio::net::RdmaHeader)
                                                                     : 0;
        }
    };

    // The session has to be established already: the handshake is a packet, and a
    // packet needs a connection.
    RdmaDeliverService(std::shared_ptr<RdmaSessionService> session, Layout layout);
    ~RdmaDeliverService() noexcept;

    RdmaDeliverService(const RdmaDeliverService&) = delete;
    RdmaDeliverService& operator=(const RdmaDeliverService&) = delete;
    RdmaDeliverService(RdmaDeliverService&&) = delete;
    RdmaDeliverService& operator=(RdmaDeliverService&&) = delete;

    // Says what this end can take and waits to hear the same from the peer. Both sides
    // do this as soon as they are connected, so neither has to know who speaks first,
    // and nothing else is sent or received until it has happened.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> handshake();

    // Starts the reader. Until this is called nothing takes packets off the session,
    // so nothing is acknowledged and nothing arrives. Called once.
    void Start();

    // Ends the link from this side: the reader is cancelled and whatever is waiting for
    // a packet is told that none is coming. The reader is a root coroutine, and a live
    // root keeps a thread's Run() from returning -- so this is also what lets a stopped
    // service be destroyed, since the reader's own frame holds a reference to it.
    void Stop() noexcept;

    // One payload, cut into as many packets as it needs. Waits for the peer to
    // acknowledge enough that the packets this adds stay inside the window, which is
    // the backpressure: a consumer that is slow simply does not get more.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> Send(std::span<const char> payload);

    // The next payload the peer sent, empty once the link is over.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<std::optional<std::span<char>>, std::string>> Receive();

    // Gives a payload back, which puts its chunk back in the pool -- and that is what
    // lets the peer send more, so the acknowledgement goes out here rather than when
    // the bytes arrived.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> release(std::span<char> payload);

    // What the peer has acknowledged, and what this end has sent: the distance between
    // them is what has to stay inside the peer's chunk count.
    std::uint64_t acknowledged() const noexcept { return acknowledged_; }

    std::uint64_t sent() const noexcept { return sent_; }

    std::uint64_t taken() const noexcept { return taken_; }

   private:
    // Everything one packet's worth: the header it arrived with, and the payload
    // inside it.
    struct Incoming {
        std::span<char> packet{};
        std::uint64_t sequence{0};
        nbio::net::RdmaPacketType type{};
        std::uint64_t acknowledge{0};
    };

    // A payload off the session and not yet asked for, with the number of the packet
    // it came in: the number is what an acknowledgement ends up naming, and it can only
    // be known when the chunk it arrived in goes back.
    struct Held {
        std::span<char> payload{};
        std::uint64_t sequence{0};
    };

    // Reads one packet off the session and says what was in it. The packet is not
    // released: its chunk goes back when its payload does.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<Incoming, std::string>> ReadPacket();
    // Works out what a packet meant: the ack inside it widens what may be sent, and a
    // payload goes on the queue for whoever asks for one.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> Absorb(Incoming packet);

    // One packet carrying `payload`, waited for: the header, the sequence, whatever
    // acknowledgement is owed, and the type the caller says it is. A meta packet is the
    // one thing sent through here that is not a payload, and its numbers mean the same
    // thing they mean on every other packet.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> SendPacket(std::span<const char> payload,
                                                                          nbio::net::RdmaPacketType type);

    // Waits until the packets already in flight leave room for `packets` more.
    nbio::async::Task<nbio::runtime, nbio::utility::expected<void, std::string>> WaitForRoom(std::uint64_t packets);

    bool RoomFor(std::uint64_t packets) const noexcept;

    // What start() runs: the reader's own loop, taking packets off the session until it
    // fails or the service is stopped. A static member rather than a lambda, so that the
    // service arrives as an ordinary parameter -- a coroutine's body is the wrong place
    // to be reading a closure.
    static nbio::async::Task<nbio::runtime, void> Read(std::shared_ptr<RdmaDeliverService> self);

    // What start() Spawned the reader as, so that stop() can cancel it. Cancelling is
    // what lets the scheduler destroy the frame, and destroying the frame is what
    // releases the reference that frame holds to this service.
    nbio::async::CoroutineToken reader_{};

    std::shared_ptr<RdmaSessionService> session_;
    Layout mine_;
    Layout peer_{};
    bool handshaken_{false};
    bool ended_{false};

    // This end's own numbering, and the two counters that decide whether it may send:
    // what it has sent, and what the peer has said it finished with.
    std::uint64_t sent_{0};
    std::uint64_t acknowledged_{0};
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
    nbio::notification::ConditionVariable room_;
    nbio::notification::ConditionVariable ready_available_;
};
}  // namespace nbio::net

#endif  // defined(__linux__)
