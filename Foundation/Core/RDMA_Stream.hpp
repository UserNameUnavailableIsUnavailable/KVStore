#pragma once
#if defined(__linux__)

#include <Foundation/Core/BitmapMemory.hpp>
#include <cstddef>
#include <cstdint>
#include <list>
#include <optional>
#include <span>
#include <system_error>

#include <rdma/rdma_cma.h>

#include "Native.hpp"

namespace Foundation::Core
{
class RDMA_Acceptor;
class RDMA_Connector;

enum class RDMA_SendStatus
{
    kDone,
    kPending,
    kPeerClosed,
    kError,
};

// A reliable-connection endpoint carrying a byte stream over SEND/RECV.
class RDMA_Stream
{
    friend class RDMA_Acceptor;
    friend class RDMA_Connector;

  public:
    using Handle = NativeHandle;

    // Both completion queues, and the most work requests either queue may hold.
    static constexpr std::uint32_t kQueueDepth{ 32 };

    // How many sends the user may have in flight, which is also the most of them
    // the device can be holding at once.
    static constexpr std::uint32_t kSendChunks{ kQueueDepth };

    static constexpr std::uint32_t kReceiveChunks{ kQueueDepth };
    // static_assert(kReceiveChunks > kSendChunks,
    //               "a receiver that posts only as many receives as the sender can fill has nothing left to land in");

    RDMA_Stream(const RDMA_Stream &) = delete;
    RDMA_Stream &operator=(const RDMA_Stream &) = delete;
    RDMA_Stream(RDMA_Stream &&other) noexcept;
    RDMA_Stream &operator=(RDMA_Stream &&other) noexcept;
    ~RDMA_Stream() noexcept;

    friend void swap(RDMA_Stream &lhs, RDMA_Stream &rhs) noexcept;

    // A chunk to fill, then hand to send(). Several chunks may be acquired at
    // once so multiple sends can be in flight.
    std::optional<std::span<char>> acquire() noexcept;

    // Posts the first `length` bytes of `chunk`. The HCA owns them until the
    // completion arrives, so the caller must not touch them until then. Returns
    // without waiting: several sends may be in flight at once.
    std::error_code send(std::span<char> chunk, std::size_t length);

    // Sends that have been posted and not yet reaped. Every one of them is
    // holding a chunk the caller cannot use again yet.
    std::size_t outstanding_sends() const noexcept
    {
        return pending_send_chunks_.size();
    }

    // A chunk the peer filled, to read and then hand back. Empty until data has
    // arrived and been polled.
    std::optional<std::span<char>> receive() noexcept;

    // Returns a received chunk and reposts it for the next message.
    void release(std::span<char> chunk);

    // Reaps send completions. 0 polls, negative blocks.
    int poll_send(int timeout_ms = 0);

    // Reaps receive completions. 0 polls, negative blocks.
    int poll_receive(int timeout_ms = 0);

    // Reaps both directions. 0 polls, negative blocks.
    int poll(int timeout_ms = 0);

    // Readable whenever a completion is queued: an event, not data.
    Handle native_handle() const noexcept;

    bool peer_closed() const noexcept
    {
        return peer_closed_;
    }

    const std::error_code &error() const noexcept
    {
        return error_code_;
    }

    const char *error_message() const noexcept;

  private:
    RDMA_Stream(::rdma_cm_id *communication_id, ::ibv_pd *protection_domain, std::uint32_t send_lkey,
                std::uint32_t receive_lkey, BitmapMemory &send_pool, BitmapMemory &receive_pool);

    // Post all receive chunks.
    void post_all_receives();

    void arm_completion_queues();
    void drain_completion_events();
    void ack_completion_events(::ibv_cq *queue);
    int poll_completion_queue(::ibv_cq *queue, int timeout_ms);

    void handle_completion(const struct ::ibv_wc &completion) noexcept;

    // Empties one completion queue and acknowledges its events, which is what
    // lets the queue be destroyed at all.
    void drain_completion_queue(::ibv_cq *queue) noexcept;

    // Hands this connection's chunks back to the shared pools.
    void reclaim_chunks() noexcept;

    // Tears down everything this stream owns.
    void reset() noexcept;

    ::rdma_cm_id *communication_id_{nullptr}; // each connection gets an id
    ::ibv_comp_channel *completion_channel_{nullptr};
    ::ibv_cq *send_completion_queue_{nullptr};
    ::ibv_cq *receive_completion_queue_{nullptr};
    ::ibv_qp *queue_pair_{nullptr};
    ::ibv_pd *protection_domain_{nullptr}; // owned by the acceptor, shared

    std::uint32_t send_lkey_{0};
    std::uint32_t receive_lkey_{0};

    BitmapMemory *send_pool_{nullptr};
    BitmapMemory *receive_pool_{nullptr};

    std::list<BitmapMemory::Chunk> free_send_chunks_; // available for acquire
    std::list<BitmapMemory::Chunk> busy_send_chunks_; // acquired, not yet posted
    std::list<BitmapMemory::Chunk> pending_send_chunks_; // posted, awaiting completion

    std::list<BitmapMemory::Chunk> free_recv_chunks_; // available to repost
    std::list<BitmapMemory::Chunk> pending_recv_chunks_; // posted, awaiting completion
    std::list<std::pair<BitmapMemory::Chunk, std::size_t>> ready_recv_chunks_; // FIFO of completed receives
    std::list<BitmapMemory::Chunk> busy_recv_chunks_; // handed to the user, waiting for release

    unsigned send_unacked_events_{0};
    unsigned receive_unacked_events_{0};
    std::error_code error_code_{};
    ::ibv_wc_status status_{IBV_WC_SUCCESS};
    bool peer_closed_{false};
};
} // namespace Foundation::Core


#endif // defined(__linux__)