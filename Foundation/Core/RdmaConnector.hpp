#pragma once
#if defined(__linux__)

#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/RdmaResourceManager.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <span>
#include <string>

#include <rdma/rdma_cma.h>

namespace Foundation::Core
{
class RdmaAcceptor;

enum class RdmaSendStatus
{
    kDone,
    kPending,
    kPeerClosed,
    kError,
};

// One end of a reliable connection: what a client makes with connect(), and what an
// acceptor hands out once it has admitted a client. It owns the id, the channel that
// id reports its connection-management events on, and the queue pair; the device,
// the protection domain, the memory regions and the chunk pools come from the
// manager it borrows, which must outlive it.
class RdmaConnector
{
    friend class RdmaAcceptor;

  public:
    using Handle = int;

    // The client's end: creates the id and the channel its own events arrive on.
    // Nothing is connected yet -- bind() pins the local address if it matters, and
    // connect() finishes the handshake. Only construction may throw, and it does
    // when the device refuses an id or a channel.
    explicit RdmaConnector(RdmaResourceManager &resources);

    // Pins the local address this connection comes from, before connecting. Left
    // out, the kernel picks along the route.
    expected<void, std::string> bind(const SocketAddress &local) noexcept;

    // Resolves the peer, builds the queue pair and finishes the handshake. What is
    // left is a connection that can send, receive and close.
    expected<void, std::string> connect(SocketAddress peer) noexcept;

    // Both completion queues, and the most work requests either queue may hold.
    static constexpr std::uint32_t kQueueDepth{ 32 };

    // How many sends the user may have in flight, which is also the most of them
    // the device can be holding at once.
    static constexpr std::uint32_t kSendChunks{ kQueueDepth };

    static constexpr std::uint32_t kReceiveChunks{ kQueueDepth };
    // static_assert(kReceiveChunks > kSendChunks,
    //               "a receiver that posts only as many receives as the sender can fill has nothing left to land in");

    RdmaConnector(const RdmaConnector &) = delete;
    RdmaConnector &operator=(const RdmaConnector &) = delete;
    RdmaConnector(RdmaConnector &&other) noexcept;
    RdmaConnector &operator=(RdmaConnector &&other) noexcept;
    ~RdmaConnector() noexcept;

    friend void swap(RdmaConnector &lhs, RdmaConnector &rhs) noexcept;

    // A chunk to fill, then hand to send(). Several chunks may be acquired at
    // once so multiple sends can be in flight. An empty answer is not a
    // failure: every chunk is either in flight or already filled, and the
    // caller can come back once a completion has retired one.
    expected<std::optional<std::span<char>>, std::string> acquire() noexcept;

    // Posts the first `length` bytes of `chunk`. The HCA owns them until the
    // completion arrives, so the caller must not touch them until then. Returns
    // without waiting: several sends may be in flight at once.
    expected<void, std::string> send(std::span<char> chunk, std::size_t length) noexcept;

    // Sends that have been posted and not yet reaped. Every one of them is
    // holding a chunk the caller cannot use again yet.
    std::size_t outstanding_sends() const noexcept
    {
        return pending_send_chunks_.size();
    }

    // A chunk the peer filled, to read and then hand back. Empty until data has
    // arrived and been polled.
    expected<std::optional<std::span<char>>, std::string> receive() noexcept;

    // Returns a received chunk and reposts it for the next message.
    expected<void, std::string> release(std::span<char> chunk) noexcept;

    // Reaps send completions and answers how many were reaped. 0 polls,
    // negative blocks. A stream that has already failed refuses to poll instead
    // of pretending its completions mean anything.
    expected<std::size_t, std::string> poll_send(int timeout_ms = 0) noexcept;

    // Reaps receive completions. 0 polls, negative blocks.
    expected<std::size_t, std::string> poll_receive(int timeout_ms = 0) noexcept;

    // Reaps both directions. 0 polls, negative blocks.
    expected<std::size_t, std::string> poll(int timeout_ms = 0) noexcept;

    // Readable whenever a completion is queued: an event, not data.
    Handle native_handle() const noexcept;

    // Readable while a connection-management event is queued for this connection:
    // the handshake finishing, a rejection, or the peer going away. An event, not
    // data.
    Handle cm_handle() const noexcept;

    // Ends the connection: disconnects, then gives up the id, the channel, the
    // queue pair and the chunks. The destructor calls it.
    void close() noexcept;

    bool peer_closed() const noexcept
    {
        return peer_closed_;
    }

    // True once anything has gone wrong with this stream, and then for good:
    // `error()` says what went wrong the first time.
    bool failed() const noexcept
    {
        return !error_.empty();
    }

    // Why the stream failed, and empty while it has not. The device reports its
    // failures as status codes rather than errno values, so this is text rather
    // than a `std::error_code`.
    const std::string &error() const noexcept
    {
        return error_;
    }

  private:
    // The acceptor's end: an id the kernel created for a connection that asked to be
    // admitted, and a channel that id has been migrated onto, so that this
    // connection's events arrive here rather than at the listener.
    RdmaConnector(::rdma_cm_id *communication_id, ::rdma_event_channel *event_channel,
                  RdmaResourceManager &resources);

    // Waits, on this connection's own channel, for the event that says the handshake
    // finished. Nothing else is something to report: the id is the only one on this
    // channel, so an event that is not the established one is that connection's
    // failure.
    expected<void, std::string> await_established() noexcept;

    // Creates the completion channel, the two completion queues and the queue pair,
    // takes this connection's chunks from the pools and posts its receives. Throws:
    // the callers are the acceptor's constructor path, where a failure is a
    // rejected connection, and connect(), where it is a failed one.
    void build_queue_pair();

    // Gives back everything build_queue_pair() took, leaving the id and the channel
    // with this object.
    void release_device_objects() noexcept;

    // Post all receive chunks.
    void post_all_receives() noexcept;

    // Asks both completion queues to raise another event. The only thing in
    // this class a provider can refuse, which is why it is the one helper whose
    // failure is reported rather than recorded.
    expected<void, std::string> arm_completion_queues() noexcept;
    void drain_completion_events() noexcept;
    void ack_completion_events(::ibv_cq *queue) noexcept;
    expected<std::size_t, std::string> poll_completion_queue(::ibv_cq *queue, int timeout_ms) noexcept;

    void handle_completion(const struct ::ibv_wc &completion) noexcept;

    // Empties one completion queue and acknowledges its events, which is what
    // lets the queue be destroyed at all.
    void drain_completion_queue(::ibv_cq *queue) noexcept;

    // Records the first failure and leaves the stream closed for business.
    // Later failures are dropped: the first one is the cause, the rest are its
    // consequences.
    void fail(std::string message) noexcept;

    // Hands this connection's chunks back to the shared pools.
    void reclaim_chunks() noexcept;

    // Tears down everything this stream owns.
    void reset() noexcept;

    ::rdma_cm_id *communication_id_{nullptr}; // each connection gets an id
    ::rdma_event_channel *event_channel_{nullptr}; // where that id reports its events
    ::ibv_comp_channel *completion_channel_{nullptr};
    ::ibv_cq *send_completion_queue_{nullptr};
    ::ibv_cq *receive_completion_queue_{nullptr};
    ::ibv_qp *queue_pair_{nullptr};

    // Borrowed for the whole life of the connection: the device, the protection
    // domain, the two registered regions and the two pools this connection's chunks
    // come from.
    RdmaResourceManager *resources_{nullptr};

    std::uint32_t send_lkey_{0};
    std::uint32_t receive_lkey_{0};

    std::list<BitmapMemory::Chunk> free_send_chunks_; // available for acquire
    std::list<BitmapMemory::Chunk> busy_send_chunks_; // acquired, not yet posted
    std::list<BitmapMemory::Chunk> pending_send_chunks_; // posted, awaiting completion

    std::list<BitmapMemory::Chunk> free_recv_chunks_; // available to repost
    std::list<BitmapMemory::Chunk> pending_recv_chunks_; // posted, awaiting completion
    std::list<std::pair<BitmapMemory::Chunk, std::size_t>> ready_recv_chunks_; // FIFO of completed receives
    std::list<BitmapMemory::Chunk> busy_recv_chunks_; // handed to the user, waiting for release

    unsigned send_unacked_events_{0};
    unsigned receive_unacked_events_{0};
    std::string error_{}; // empty until the first failure, then the reason
    bool peer_closed_{false};
};
} // namespace Foundation::Core


#endif // defined(__linux__)