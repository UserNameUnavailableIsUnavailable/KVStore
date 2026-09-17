#include <system_error>
#if defined(__linux__)

#include <Foundation/Core/BitmapMemory.hpp>
#include <infiniband/verbs.h>
#include "RDMA_Stream.hpp"

#include <algorithm>
#include <cassert>
#include <fcntl.h>
#include <poll.h>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace Foundation::Core
{
RDMA_Stream::RDMA_Stream(::rdma_cm_id *communication_id, ::ibv_pd *protection_domain, std::uint32_t send_lkey,
                         std::uint32_t receive_lkey, BitmapMemory &send_pool, BitmapMemory &receive_pool) :
    communication_id_(communication_id),
    protection_domain_(protection_domain), send_lkey_(send_lkey), receive_lkey_(receive_lkey),
    send_pool_(&send_pool), receive_pool_(&receive_pool)
{
    if (!communication_id_ || !protection_domain_)
    {
        throw std::invalid_argument("RDMA stream requires a communication id and protection domain");
    }

    completion_channel_ = ::ibv_create_comp_channel(communication_id_->verbs);
    if (!completion_channel_)
    {
        throw std::runtime_error("Failed to create RDMA completion channel");
    }

    // poll() drains events until ibv_get_cq_event reports EAGAIN, so the fd has
    // to be non-blocking or that loop never returns.
    const int flags = ::fcntl(completion_channel_->fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(completion_channel_->fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        ::ibv_destroy_comp_channel(completion_channel_);
        completion_channel_ = nullptr;
        throw std::runtime_error("Failed to make the RDMA completion channel non-blocking");
    }

    try
    {
        send_completion_queue_ =
            ::ibv_create_cq(communication_id_->verbs, kQueueDepth, nullptr, completion_channel_, 0);
        if (!send_completion_queue_)
        {
            throw std::runtime_error("Failed to create RDMA send completion queue");
        }
        receive_completion_queue_ =
            ::ibv_create_cq(communication_id_->verbs, kQueueDepth, nullptr, completion_channel_, 0);
        if (!receive_completion_queue_)
        {
            throw std::runtime_error("Failed to create RDMA receive completion queue");
        }

        ibv_qp_init_attr attributes{
            .qp_context = nullptr,
            .send_cq = send_completion_queue_,
            .recv_cq = receive_completion_queue_,
            .srq = nullptr, // FIXME: maybe SRQ is a better choice than fixed receive queue?
            .cap =
                {
                    .max_send_wr = kQueueDepth,
                    .max_recv_wr = kQueueDepth,
                    .max_send_sge = 1,
                    .max_recv_sge = 1,
                    .max_inline_data = 0,
                },
            .qp_type = IBV_QPT_RC,
            .sq_sig_all = 0,
        };
        if (::rdma_create_qp(communication_id_, protection_domain_, &attributes) != 0)
        {
            throw std::runtime_error("Failed to create RDMA queue pair");
        }
        queue_pair_ = communication_id_->qp;

        arm_completion_queues();

        for (std::uint32_t i = 0; i < kSendChunks; ++i)
        {
            auto chunk = send_pool_->acquire();
            if (!chunk)
            {
                throw std::runtime_error("RDMA send pool cannot admit another connection");
            }
            free_send_chunks_.push_back(*chunk);
        }
        for (std::uint32_t i = 0; i < kReceiveChunks; ++i)
        {
            auto chunk = receive_pool_->acquire();
            if (!chunk)
            {
                throw std::runtime_error("RDMA receive pool cannot admit another connection");
            }
            free_recv_chunks_.push_back(*chunk);
        }

        // Post all receive chunks, otherwise incoming sends will fail.
        post_all_receives();
    }
    catch (...)
    {
        if (queue_pair_)
        {
            ::rdma_destroy_qp(communication_id_);
            queue_pair_ = nullptr;
        }
        reclaim_chunks();
        if (send_completion_queue_)
        {
            ::ibv_destroy_cq(send_completion_queue_);
            send_completion_queue_ = nullptr;
        }
        if (receive_completion_queue_)
        {
            ::ibv_destroy_cq(receive_completion_queue_);
            receive_completion_queue_ = nullptr;
        }
        if (completion_channel_)
        {
            ::ibv_destroy_comp_channel(completion_channel_);
            completion_channel_ = nullptr;
        }
        // The communication id is left alone: the acceptor still needs it to
        // reject the connection.
        throw;
    }
}

RDMA_Stream::RDMA_Stream(RDMA_Stream &&other) noexcept
{
    swap(*this, other);
}

RDMA_Stream &RDMA_Stream::operator=(RDMA_Stream &&other) noexcept
{
    if (this != &other)
    {
        reset();
        swap(*this, other);
    }
    return *this;
}

void swap(RDMA_Stream &lhs, RDMA_Stream &rhs) noexcept
{
    using std::swap;
    swap(lhs.communication_id_, rhs.communication_id_);
    swap(lhs.completion_channel_, rhs.completion_channel_);
    swap(lhs.send_completion_queue_, rhs.send_completion_queue_);
    swap(lhs.receive_completion_queue_, rhs.receive_completion_queue_);
    swap(lhs.queue_pair_, rhs.queue_pair_);
    swap(lhs.protection_domain_, rhs.protection_domain_);
    swap(lhs.send_lkey_, rhs.send_lkey_);
    swap(lhs.receive_lkey_, rhs.receive_lkey_);
    swap(lhs.send_pool_, rhs.send_pool_);
    swap(lhs.receive_pool_, rhs.receive_pool_);
    swap(lhs.free_send_chunks_, rhs.free_send_chunks_);
    swap(lhs.busy_send_chunks_, rhs.busy_send_chunks_);
    swap(lhs.pending_send_chunks_, rhs.pending_send_chunks_);
    swap(lhs.free_recv_chunks_, rhs.free_recv_chunks_);
    swap(lhs.pending_recv_chunks_, rhs.pending_recv_chunks_);
    swap(lhs.ready_recv_chunks_, rhs.ready_recv_chunks_);
    swap(lhs.busy_recv_chunks_, rhs.busy_recv_chunks_);
    swap(lhs.send_unacked_events_, rhs.send_unacked_events_);
    swap(lhs.receive_unacked_events_, rhs.receive_unacked_events_);
    swap(lhs.error_code_, rhs.error_code_);
    swap(lhs.status_, rhs.status_);
    swap(lhs.peer_closed_, rhs.peer_closed_);
}

RDMA_Stream::~RDMA_Stream() noexcept
{
    reset();
}

void RDMA_Stream::reset() noexcept
{
    // Destroying the queue pair discards outstanding work requests, so it has to
    // happen before the chunks go back to the pool.
    if (queue_pair_)
    {
        ::rdma_destroy_qp(communication_id_);
        queue_pair_ = nullptr;
    }
    reclaim_chunks();

    // IMPORTANT: every completion event returned by ibv_get_cq_event() must be acknowledged before destroying the CQ.
    // Otherwise, ibv_destroy_cq() will block indefinitely waiting for those acknowledgmentsdrain_completion_queue(send_completion_queue_);
    // Empty both queues and acknowledge every event before destroying them. A
    // provider will not finish tearing a completion queue down while it still
    // has completions or unacknowledged events outstanding -- siw waits inside
    // ibv_destroy_cq for them -- so a link that is closed right after its last
    // completion would hang here instead of returning. Nothing reads these
    // completions: the queue pair is gone and every chunk has been reclaimed.
    drain_completion_queue(send_completion_queue_);
    drain_completion_queue(receive_completion_queue_);
    if (receive_completion_queue_)
    {
        ::ibv_destroy_cq(receive_completion_queue_);
        receive_completion_queue_ = nullptr;
    }
    if (send_completion_queue_)
    {
        ::ibv_destroy_cq(send_completion_queue_);
        send_completion_queue_ = nullptr;
    }
    if (completion_channel_)
    {
        ::ibv_destroy_comp_channel(completion_channel_);
        completion_channel_ = nullptr;
    }
    if (communication_id_)
    {
        ::rdma_destroy_id(communication_id_);
        communication_id_ = nullptr;
    }
    peer_closed_ = true;
}

void RDMA_Stream::drain_completion_queue(::ibv_cq *queue) noexcept
{
    if (!queue || !completion_channel_)
    {
        return;
    }

    ibv_wc completions[kQueueDepth];
    while (::ibv_poll_cq(queue, kQueueDepth, completions) > 0)
    {
    }

    // Events are counted as the completion channel hands them over and only ever
    // acknowledged where they are consumed, so whatever is left is acknowledged
    // here.
    unsigned &unacked = queue == send_completion_queue_ ? send_unacked_events_ : receive_unacked_events_;
    if (unacked != 0)
    {
        ::ibv_ack_cq_events(queue, unacked);
        unacked = 0;
    }
}

void RDMA_Stream::reclaim_chunks() noexcept
{
    if (!send_pool_ || !receive_pool_)
    {
        return;
    }
    for (const auto chunk : free_send_chunks_)
    {
        send_pool_->release(chunk);
    }
    for (const auto chunk : busy_send_chunks_)
    {
        send_pool_->release(chunk);
    }
    for (const auto chunk : pending_send_chunks_)
    {
        send_pool_->release(chunk);
    }
    for (const auto chunk : free_recv_chunks_)
    {
        receive_pool_->release(chunk);
    }
    for (const auto chunk : busy_recv_chunks_)
    {
        receive_pool_->release(chunk);
    }
    for (const auto chunk : pending_recv_chunks_)
    {
        receive_pool_->release(chunk);
    }
    for (const auto &ready : ready_recv_chunks_)
    {
        receive_pool_->release(ready.first);
    }

    free_send_chunks_.clear();
    busy_send_chunks_.clear();
    pending_send_chunks_.clear();
    free_recv_chunks_.clear();
    busy_recv_chunks_.clear();
    pending_recv_chunks_.clear();
    ready_recv_chunks_.clear();
}

RDMA_Stream::Handle RDMA_Stream::native_handle() const noexcept
{
    return completion_channel_ ? completion_channel_->fd : -1;
}

const char *RDMA_Stream::error_message() const noexcept
{
    return ::ibv_wc_status_str(status_);
}

std::optional<std::span<char>> RDMA_Stream::acquire() noexcept
{
    if (free_send_chunks_.empty())
    {
        return std::nullopt;
    }
    auto ret = free_send_chunks_.front();
    busy_send_chunks_.splice(busy_send_chunks_.end(), free_send_chunks_, free_send_chunks_.begin());
    return send_pool_->data(ret);
}

std::error_code RDMA_Stream::send(std::span<char> chunk, std::size_t length)
{
    if (error_code_)
    {
        return error_code_;
    }

    auto exists = [this, &chunk](BitmapMemory::Chunk other) {
        return chunk.data() == send_pool_->data(other).data();
    };
    auto it = std::find_if(busy_send_chunks_.begin(), busy_send_chunks_.end(), exists);
    if (it == busy_send_chunks_.end())
    {
        throw std::logic_error("must send with an acquired chunk");
    }
    if (length > chunk.size())
    {
        return std::make_error_code(std::errc::message_size);
    }
    if (length == 0)
    {
        free_send_chunks_.splice(free_send_chunks_.end(), busy_send_chunks_, it);
        return {};
    }

    ibv_sge segment{.addr = reinterpret_cast<std::uintptr_t>(chunk.data()),
                    .length = static_cast<std::uint32_t>(length),
                    .lkey = send_lkey_};
    ibv_send_wr request{};
    request.wr_id = it->index;
    request.sg_list = &segment;
    request.num_sge = 1;
    request.opcode = IBV_WR_SEND;
    request.send_flags = IBV_SEND_SIGNALED;

    ibv_send_wr *rejected{nullptr};
    if (const int code = ::ibv_post_send(queue_pair_, &request, &rejected); code != 0)
    {
        return {code, std::system_category()};
    }

    // The HCA owns the chunk until its completion retires it.
    pending_send_chunks_.splice(pending_send_chunks_.end(), busy_send_chunks_, it);
    return {};
}

std::optional<std::span<char>> RDMA_Stream::receive() noexcept
{
    if (ready_recv_chunks_.empty())
    {
        return std::nullopt;
    }
    auto [chunk, size] = ready_recv_chunks_.front();
    ready_recv_chunks_.pop_front();
    busy_recv_chunks_.push_back(chunk);
    auto data = receive_pool_->data(chunk);
    return data.first(std::min(size, data.size()));
}

void RDMA_Stream::release(std::span<char> chunk)
{
    auto it = std::find_if(busy_recv_chunks_.begin(), busy_recv_chunks_.end(), [this, &chunk](BitmapMemory::Chunk other) {
        return chunk.data() == receive_pool_->data(other).data();
    });
    if (it == busy_recv_chunks_.end())
    {
        return;
    }
    free_recv_chunks_.splice(free_recv_chunks_.end(), busy_recv_chunks_, it);
    post_all_receives();
}

void RDMA_Stream::arm_completion_queues()
{
    if (send_completion_queue_ && ::ibv_req_notify_cq(send_completion_queue_, 0) != 0)
    {
        throw std::runtime_error("Failed to arm RDMA send completion queue");
    }
    if (receive_completion_queue_ && ::ibv_req_notify_cq(receive_completion_queue_, 0) != 0)
    {
        throw std::runtime_error("Failed to arm RDMA receive completion queue");
    }
}

void RDMA_Stream::drain_completion_events()
{
    if (!completion_channel_)
    {
        return;
    }

    ::ibv_cq *queue{nullptr};
    void *context{nullptr};
    while (::ibv_get_cq_event(completion_channel_, &queue, &context) == 0)
    {
        if (queue == send_completion_queue_)
        {
            ++send_unacked_events_;
        }
        else if (queue == receive_completion_queue_)
        {
            ++receive_unacked_events_;
        }
    }
}

void RDMA_Stream::ack_completion_events(::ibv_cq *queue)
{
    if (queue == send_completion_queue_ && send_unacked_events_ != 0)
    {
        ::ibv_ack_cq_events(send_completion_queue_, send_unacked_events_);
        send_unacked_events_ = 0;
    }
    else if (queue == receive_completion_queue_ && receive_unacked_events_ != 0)
    {
        ::ibv_ack_cq_events(receive_completion_queue_, receive_unacked_events_);
        receive_unacked_events_ = 0;
    }
}

int RDMA_Stream::poll_completion_queue(::ibv_cq *queue, int timeout_ms)
{
    if (!queue || !completion_channel_)
    {
        return 0;
    }

    arm_completion_queues();

    if (timeout_ms != 0)
    {
        pollfd waiter{.fd = completion_channel_->fd, .events = POLLIN, .revents = 0};
        (void)::poll(&waiter, 1, timeout_ms);
    }

    drain_completion_events();
    ack_completion_events(queue);
    arm_completion_queues();

    int total = 0;
    ibv_wc completions[kQueueDepth];
    while (true)
    {
        const int count = ::ibv_poll_cq(queue, kQueueDepth, completions);
        if (count <= 0)
        {
            break;
        }
        for (int i = 0; i < count; ++i)
        {
            handle_completion(completions[i]);
        }
        total += count;
    }
    return total;
}

void RDMA_Stream::post_all_receives()
{
    if (error_code_)
    {
        return;
    }
    while (!free_recv_chunks_.empty())
    {
        const auto chunk = free_recv_chunks_.front();
        free_recv_chunks_.pop_front();

        const auto bytes = receive_pool_->data(chunk);
        ::ibv_sge segment{.addr = reinterpret_cast<std::uintptr_t>(bytes.data()),
                        .length = static_cast<std::uint32_t>(bytes.size()),
                        .lkey = receive_lkey_};

        ::ibv_recv_wr request{};
        request.wr_id = chunk.encode();
        request.sg_list = &segment;
        request.num_sge = 1;

        ::ibv_recv_wr *rejected{nullptr};
        if (const int code = ::ibv_post_recv(queue_pair_, &request, &rejected); code != 0)
        {
            free_recv_chunks_.push_front(chunk);
            error_code_ = std::error_code(code, std::system_category());
            return;
        }
        pending_recv_chunks_.push_back(chunk);
    }
}

void RDMA_Stream::handle_completion(const ::ibv_wc &completion) noexcept
{
    if (completion.status != IBV_WC_SUCCESS) [[unlikely]]
    {
        peer_closed_ = true;
        if (completion.status != IBV_WC_WR_FLUSH_ERR) // shutdown
        {
            status_ = completion.status;
        }
        const auto chunk = BitmapMemory::Chunk::decode(completion.wr_id);
        auto remove = [chunk](std::list<BitmapMemory::Chunk> &list) {
            auto it = std::find(list.begin(), list.end(), chunk);
            if (it == list.end())
            {
                return false;
            }
            list.erase(it);
            return true;
        };
        if (completion.opcode & IBV_WC_SEND)
        {
            if (!remove(pending_send_chunks_))
            {
                remove(busy_send_chunks_);
            }
            free_send_chunks_.push_back(chunk);
        }
        else if (completion.opcode & IBV_WC_RECV)
        {
            if (!remove(pending_recv_chunks_))
            {
                remove(busy_recv_chunks_);
            }
            free_recv_chunks_.push_back(chunk);
        }
        return;
    }

    const auto chunk = BitmapMemory::Chunk::decode(completion.wr_id);
    if (completion.opcode == IBV_WC_SEND)
    {
        auto it = std::find(pending_send_chunks_.begin(), pending_send_chunks_.end(), chunk);
        if (it != pending_send_chunks_.end())
        {
            free_send_chunks_.splice(free_send_chunks_.end(), pending_send_chunks_, it);
        }
    }
    else if (completion.opcode == IBV_WC_RECV)
    {
        auto it = std::find(pending_recv_chunks_.begin(), pending_recv_chunks_.end(), chunk);
        if (it != pending_recv_chunks_.end())
        {
            const auto size = static_cast<std::size_t>(completion.byte_len);
            pending_recv_chunks_.erase(it);
            ready_recv_chunks_.emplace_back(chunk, size);
        }
    }
    else [[unlikely]]
    {
        assert(false && "unexpected opcode");
    }
}

int RDMA_Stream::poll_send(int timeout_ms)
{
    return poll_completion_queue(send_completion_queue_, timeout_ms);
}

int RDMA_Stream::poll_receive(int timeout_ms)
{
    return poll_completion_queue(receive_completion_queue_, timeout_ms);
}

int RDMA_Stream::poll(int timeout_ms)
{
    const int send = poll_send(timeout_ms);
    const int receive = poll_receive(0);
    return send + receive;
}
} // namespace Foundation::Core

#endif // defined(__linux__)