#if defined(__linux__)

#include <Foundation/Core/BitmapMemory.hpp>
#include <infiniband/verbs.h>
#include "RdmaConnector.hpp"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>

namespace Foundation::Core
{
namespace
{
// The verbs and rdma_cm calls report a failure as a nonzero return plus errno, so
// every message this file builds ends the same way.
std::string Failing(std::string_view what)
{
    return std::string{ what } + ": " + ::strerror(errno);
}

// Waits for one event on a connection's own channel. The channels a connection
// owns are non-blocking, because an event loop may be watching them, so this polls
// the fd and then takes the event rather than blocking inside rdma_get_cm_event.
// The caller acknowledges what comes back.
expected<::rdma_cm_event *, std::string> WaitCmEvent(::rdma_event_channel *channel, int timeout_ms) noexcept
{
    pollfd waiter{.fd = channel->fd, .events = POLLIN, .revents = 0};
    if (::poll(&waiter, 1, timeout_ms) <= 0) [[unlikely]]
    {
        return unexpected(std::string{ "An RDMA connection event did not arrive" });
    }
    ::rdma_cm_event *event{nullptr};
    if (::rdma_get_cm_event(channel, &event) != 0) [[unlikely]]
    {
        return unexpected(Failing("Failed to get an RDMA CM event"));
    }
    return event;
}
} // namespace

RdmaConnector::RdmaConnector(::rdma_cm_id *communication_id, ::rdma_event_channel *event_channel,
                             RdmaResourceManager &resources) :
    communication_id_(communication_id), event_channel_(event_channel), resources_(&resources),
    send_lkey_(resources.send_region()->lkey), receive_lkey_(resources.receive_region()->lkey)
{
    if (!communication_id_ || !event_channel_)
    {
        throw std::invalid_argument("an RDMA connection needs a communication id and a channel of its own");
    }
    build_queue_pair();
}

RdmaConnector::RdmaConnector(RdmaResourceManager &resources) : resources_(&resources)
{
    // The channel comes first, because the id is created on it, and it is the
    // channel this connection's own events -- established, rejected, disconnected --
    // arrive on for the rest of its life.
    event_channel_ = ::rdma_create_event_channel();
    if (!event_channel_) [[unlikely]]
    {
        throw std::runtime_error(Failing("Failed to create an RDMA event channel"));
    }
    if (::rdma_create_id(event_channel_, &communication_id_, nullptr, RDMA_PS_TCP) != 0) [[unlikely]]
    {
        const auto why = Failing("Failed to create an RDMA id");
        ::rdma_destroy_event_channel(event_channel_);
        event_channel_ = nullptr;
        throw std::runtime_error(why);
    }
    // The keys are the manager's to give, and they are the same for every
    // connection that shares its regions.
    send_lkey_ = resources.send_region()->lkey;
    receive_lkey_ = resources.receive_region()->lkey;
}

expected<void, std::string> RdmaConnector::bind(const SocketAddress &local) noexcept
{
    if (::rdma_bind_addr(communication_id_, const_cast<::sockaddr *>(local.storage<::sockaddr>())) != 0) [[unlikely]]
    {
        return unexpected(Failing("Failed to bind the RDMA address"));
    }
    return {};
}

void RdmaConnector::build_queue_pair()
{
    try
    {
        // The completion channel comes first, because it is what the queues report
        // on and what an event loop watches for them. Both queues share it: a
        // completion in either direction makes its one fd readable, which is why
        // both directions are polled from the same event.
        completion_channel_ = ::ibv_create_comp_channel(communication_id_->verbs);
        if (!completion_channel_)
        {
            throw std::runtime_error("Failed to create RDMA completion channel");
        }
        // Draining the queue reads events until ibv_get_cq_event reports EAGAIN, so
        // the fd has to be non-blocking or that loop never returns -- and a driver
        // that hands an event to a blocking reader would stop the event loop instead
        // of waking it.
        const int flags = ::fcntl(completion_channel_->fd, F_GETFL, 0);
        if (flags < 0 || ::fcntl(completion_channel_->fd, F_SETFL, flags | O_NONBLOCK) < 0) [[unlikely]]
        {
            throw std::runtime_error("Failed to make the RDMA completion channel non-blocking");
        }

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
        if (::rdma_create_qp(communication_id_, resources_->protection_domain(), &attributes) != 0)
        {
            throw std::runtime_error("Failed to create RDMA queue pair");
        }
        queue_pair_ = communication_id_->qp;

        // A stream whose queues are not armed can never be polled, and the
        // constructor is the one place allowed to throw, so the failure
        // unwinds here instead of being carried as state.
        if (auto armed = arm_completion_queues(); !armed) [[unlikely]]
        {
            throw std::runtime_error(armed.error());
        }

        for (std::uint32_t i = 0; i < kSendChunks; ++i)
        {
            auto chunk = resources_->send_memory().acquire();
            if (!chunk)
            {
                throw std::runtime_error("RDMA send pool cannot admit another connection");
            }
            free_send_chunks_.push_back(*chunk);
        }
        for (std::uint32_t i = 0; i < kReceiveChunks; ++i)
        {
            auto chunk = resources_->receive_memory().acquire();
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
        // The id and the channel stay: the acceptor rejects the connection with the
        // one and this object's destructor gives back the other.
        release_device_objects();
        throw;
    }
}

void RdmaConnector::release_device_objects() noexcept
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
}

RdmaConnector::RdmaConnector(RdmaConnector &&other) noexcept
{
    swap(*this, other);
}

RdmaConnector &RdmaConnector::operator=(RdmaConnector &&other) noexcept
{
    if (this != &other)
    {
        reset();
        swap(*this, other);
    }
    return *this;
}

void swap(RdmaConnector &lhs, RdmaConnector &rhs) noexcept
{
    using std::swap;
    swap(lhs.communication_id_, rhs.communication_id_);
    swap(lhs.event_channel_, rhs.event_channel_);
    swap(lhs.completion_channel_, rhs.completion_channel_);
    swap(lhs.send_completion_queue_, rhs.send_completion_queue_);
    swap(lhs.receive_completion_queue_, rhs.receive_completion_queue_);
    swap(lhs.queue_pair_, rhs.queue_pair_);
    swap(lhs.resources_, rhs.resources_);
    swap(lhs.send_lkey_, rhs.send_lkey_);
    swap(lhs.receive_lkey_, rhs.receive_lkey_);
    swap(lhs.free_send_chunks_, rhs.free_send_chunks_);
    swap(lhs.busy_send_chunks_, rhs.busy_send_chunks_);
    swap(lhs.pending_send_chunks_, rhs.pending_send_chunks_);
    swap(lhs.free_recv_chunks_, rhs.free_recv_chunks_);
    swap(lhs.pending_recv_chunks_, rhs.pending_recv_chunks_);
    swap(lhs.ready_recv_chunks_, rhs.ready_recv_chunks_);
    swap(lhs.busy_recv_chunks_, rhs.busy_recv_chunks_);
    swap(lhs.send_unacked_events_, rhs.send_unacked_events_);
    swap(lhs.receive_unacked_events_, rhs.receive_unacked_events_);
    swap(lhs.error_, rhs.error_);
    swap(lhs.peer_closed_, rhs.peer_closed_);
}

RdmaConnector::~RdmaConnector() noexcept
{
    reset();
}

void RdmaConnector::reset() noexcept
{
    // A disconnect first, so the peer is told the connection is over rather than
    // left with a queue pair that went quiet. Its disconnect event is the peer's
    // to read on its own channel.
    if (communication_id_ && queue_pair_)
    {
        ::rdma_disconnect(communication_id_);
    }
    // Destroying the queue pair discards outstanding work requests, so it has to
    // happen before the chunks go back to the pool.
    if (queue_pair_)
    {
        ::rdma_destroy_qp(communication_id_);
        queue_pair_ = nullptr;
    }
    reclaim_chunks();

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
    if (event_channel_)
    {
        ::rdma_destroy_event_channel(event_channel_);
        event_channel_ = nullptr;
    }
    peer_closed_ = true;
}

void RdmaConnector::drain_completion_queue(::ibv_cq *queue) noexcept
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

void RdmaConnector::reclaim_chunks() noexcept
{
    if (resources_ == nullptr)
    {
        return;
    }
    for (const auto chunk : free_send_chunks_)
    {
        resources_->send_memory().release(chunk);
    }
    for (const auto chunk : busy_send_chunks_)
    {
        resources_->send_memory().release(chunk);
    }
    for (const auto chunk : pending_send_chunks_)
    {
        resources_->send_memory().release(chunk);
    }
    for (const auto chunk : free_recv_chunks_)
    {
        resources_->receive_memory().release(chunk);
    }
    for (const auto chunk : busy_recv_chunks_)
    {
        resources_->receive_memory().release(chunk);
    }
    for (const auto chunk : pending_recv_chunks_)
    {
        resources_->receive_memory().release(chunk);
    }
    for (const auto &ready : ready_recv_chunks_)
    {
        resources_->receive_memory().release(ready.first);
    }

    free_send_chunks_.clear();
    busy_send_chunks_.clear();
    pending_send_chunks_.clear();
    free_recv_chunks_.clear();
    busy_recv_chunks_.clear();
    pending_recv_chunks_.clear();
    ready_recv_chunks_.clear();
}

RdmaConnector::Handle RdmaConnector::native_handle() const noexcept
{
    return completion_channel_ ? completion_channel_->fd : -1;
}

RdmaConnector::Handle RdmaConnector::cm_handle() const noexcept
{
    return event_channel_ ? event_channel_->fd : -1;
}

void RdmaConnector::close() noexcept
{
    reset();
}

void RdmaConnector::fail(std::string message) noexcept
{
    // The first failure is the cause and the rest are its consequences, so only
    // the first one is kept.
    if (error_.empty())
    {
        error_ = std::move(message);
    }
}

expected<std::optional<std::span<char>>, std::string> RdmaConnector::acquire() noexcept
{
    if (failed()) [[unlikely]]
    {
        return unexpected(error_);
    }
    if (queue_pair_ == nullptr) [[unlikely]]
    {
        return unexpected(std::string{ "The RDMA connection is not established" });
    }
    if (free_send_chunks_.empty())
    {
        return std::optional<std::span<char>>{};
    }
    auto ret = free_send_chunks_.front();
    busy_send_chunks_.splice(busy_send_chunks_.end(), free_send_chunks_, free_send_chunks_.begin());
    return std::optional<std::span<char>>{ resources_->send_memory().data(ret) };
}

expected<void, std::string> RdmaConnector::send(std::span<char> chunk, std::size_t length) noexcept
{
    if (failed()) [[unlikely]]
    {
        return unexpected(error_);
    }
    if (queue_pair_ == nullptr) [[unlikely]]
    {
        return unexpected(std::string{ "The RDMA connection is not established" });
    }

    auto exists = [this, &chunk](BitmapMemory::Chunk other) {
        return chunk.data() == resources_->send_memory().data(other).data();
    };
    auto it = std::find_if(busy_send_chunks_.begin(), busy_send_chunks_.end(), exists);
    if (it == busy_send_chunks_.end()) [[unlikely]]
    {
        // A caller that never acquired this chunk would otherwise post memory
        // the device does not own, so this is a programming error rather than a
        // runtime one -- but it is reported like any other failure rather than
        // thrown, because a data-plane call must not unwind.
        return unexpected(std::string{"RDMA send requires a chunk from acquire()"});
    }
    if (length > chunk.size()) [[unlikely]]
    {
        return unexpected(std::string{"RDMA send is longer than the acquired chunk"});
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
    if (const int code = ::ibv_post_send(queue_pair_, &request, &rejected); code != 0) [[unlikely]]
    {
        return unexpected(std::string{"Failed to post RDMA send: "} + ::strerror(code));
    }

    // The HCA owns the chunk until its completion retires it.
    pending_send_chunks_.splice(pending_send_chunks_.end(), busy_send_chunks_, it);
    return {};
}

expected<std::optional<std::span<char>>, std::string> RdmaConnector::receive() noexcept
{
    if (failed()) [[unlikely]]
    {
        return unexpected(error_);
    }
    if (queue_pair_ == nullptr) [[unlikely]]
    {
        return unexpected(std::string{ "The RDMA connection is not established" });
    }
    if (ready_recv_chunks_.empty())
    {
        return std::optional<std::span<char>>{};
    }
    auto [chunk, size] = ready_recv_chunks_.front();
    ready_recv_chunks_.pop_front();
    busy_recv_chunks_.push_back(chunk);
    auto data = resources_->receive_memory().data(chunk);
    return std::optional<std::span<char>>{ data.first(std::min(size, data.size())) };
}

expected<void, std::string> RdmaConnector::release(std::span<char> chunk) noexcept
{
    if (failed()) [[unlikely]]
    {
        return unexpected(error_);
    }
    if (queue_pair_ == nullptr) [[unlikely]]
    {
        return unexpected(std::string{ "The RDMA connection is not established" });
    }
    auto it = std::find_if(busy_recv_chunks_.begin(), busy_recv_chunks_.end(), [this, &chunk](BitmapMemory::Chunk other) {
        return chunk.data() == resources_->receive_memory().data(other).data();
    });
    if (it == busy_recv_chunks_.end()) [[unlikely]]
    {
        return unexpected(std::string{"RDMA release requires a chunk from receive()"});
    }
    free_recv_chunks_.splice(free_recv_chunks_.end(), busy_recv_chunks_, it);
    post_all_receives();
    if (failed()) [[unlikely]]
    {
        return unexpected(error_);
    }
    return {};
}

expected<void, std::string> RdmaConnector::arm_completion_queues() noexcept
{
    // Arming is what makes the completion channel raise an event, so a queue
    // that refuses to arm would leave poll() waiting for something that can
    // never arrive.
    if (send_completion_queue_ && ::ibv_req_notify_cq(send_completion_queue_, 0) != 0) [[unlikely]]
    {
        return unexpected(std::string{"Failed to arm the RDMA send completion queue: "} + ::strerror(errno));
    }
    if (receive_completion_queue_ && ::ibv_req_notify_cq(receive_completion_queue_, 0) != 0) [[unlikely]]
    {
        return unexpected(std::string{"Failed to arm the RDMA receive completion queue: "} + ::strerror(errno));
    }
    return {};
}

void RdmaConnector::drain_completion_events() noexcept
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

void RdmaConnector::ack_completion_events(::ibv_cq *queue) noexcept
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

expected<std::size_t, std::string> RdmaConnector::poll_completion_queue(::ibv_cq *queue, int timeout_ms) noexcept
{
    if (!queue || !completion_channel_)
    {
        return std::size_t{0};
    }
    if (failed()) [[unlikely]]
    {
        return unexpected(error_);
    }

    if (auto armed = arm_completion_queues(); !armed) [[unlikely]]
    {
        return unexpected(armed.error());
    }

    if (timeout_ms != 0)
    {
        pollfd waiter{.fd = completion_channel_->fd, .events = POLLIN, .revents = 0};
        (void)::poll(&waiter, 1, timeout_ms);
    }

    drain_completion_events();
    ack_completion_events(queue);
    if (auto armed = arm_completion_queues(); !armed) [[unlikely]]
    {
        return unexpected(armed.error());
    }

    std::size_t total = 0;
    ::ibv_wc completions[kQueueDepth];
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
        total += static_cast<std::size_t>(count);
    }

    if (failed()) [[unlikely]]
    {
        // One of those completions reported the failure that ended this stream.
        // Answering with a count would let the caller read it as progress.
        return unexpected(error_);
    }
    return total;
}

void RdmaConnector::post_all_receives() noexcept
{
    if (failed())
    {
        return;
    }
    while (!free_recv_chunks_.empty())
    {
        const auto chunk = free_recv_chunks_.front();
        free_recv_chunks_.pop_front();

        const auto bytes = resources_->receive_memory().data(chunk);
        ::ibv_sge segment{.addr = reinterpret_cast<std::uintptr_t>(bytes.data()),
                        .length = static_cast<std::uint32_t>(bytes.size()),
                        .lkey = receive_lkey_};

        ::ibv_recv_wr request{};
        request.wr_id = chunk.encode();
        request.sg_list = &segment;
        request.num_sge = 1;

        ::ibv_recv_wr *rejected{nullptr};
        if (const int code = ::ibv_post_recv(queue_pair_, &request, &rejected); code != 0) [[unlikely]]
        {
            free_recv_chunks_.push_front(chunk);
            fail(std::string{"Failed to post an RDMA receive: "} + ::strerror(code));
            return;
        }
        pending_recv_chunks_.push_back(chunk);
    }
}

void RdmaConnector::handle_completion(const ::ibv_wc &completion) noexcept
{
    if (completion.status != IBV_WC_SUCCESS) [[unlikely]]
    {
        peer_closed_ = true;
        if (completion.status != IBV_WC_WR_FLUSH_ERR) // shutdown
        {
            fail(std::string{::ibv_wc_status_str(completion.status)});
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

expected<std::size_t, std::string> RdmaConnector::poll_send(int timeout_ms) noexcept
{
    return poll_completion_queue(send_completion_queue_, timeout_ms);
}

expected<std::size_t, std::string> RdmaConnector::poll_receive(int timeout_ms) noexcept
{
    return poll_completion_queue(receive_completion_queue_, timeout_ms);
}

expected<std::size_t, std::string> RdmaConnector::poll(int timeout_ms) noexcept
{
    // Only one half waits: waiting on both in turn would make the answer depend
    // on which direction happened to arrive first. The second half is then
    // reaped without waiting, which costs nothing when it has nothing.
    auto send = poll_send(timeout_ms);
    if (!send) [[unlikely]]
    {
        return unexpected(send.error());
    }
    auto receive = poll_receive(0);
    if (!receive) [[unlikely]]
    {
        return unexpected(receive.error());
    }
    return *send + *receive;
}

expected<void, std::string> RdmaConnector::await_established() noexcept
{
    auto event = WaitCmEvent(event_channel_, 2000);
    if (!event) [[unlikely]]
    {
        return unexpected(event.error());
    }
    const bool established = (*event)->event == RDMA_CM_EVENT_ESTABLISHED;
    ::rdma_ack_cm_event(*event);
    if (!established) [[unlikely]]
    {
        return unexpected(std::string{ "The RDMA connection was not established" });
    }
    return {};
}

expected<void, std::string> RdmaConnector::connect(SocketAddress peer) noexcept
{
    const auto take_event = [this](::rdma_cm_event_type wanted) noexcept
        -> expected<void, std::string> {
        auto event = WaitCmEvent(event_channel_, 2000);
        if (!event)
        {
            return unexpected(event.error());
        }
        const auto received = (*event)->event;
        ::rdma_ack_cm_event(*event);
        if (received != wanted)
        {
            return unexpected(std::string{ "Unexpected RDMA CM event: " } + ::rdma_event_str(received));
        }
        return {};
    };

    // Resolving the address is what gives the id its device context.
    if (::rdma_resolve_addr(communication_id_, nullptr, const_cast<::sockaddr *>(peer.storage<::sockaddr>()),
                            2000) != 0) [[unlikely]]
    {
        return unexpected(Failing("Failed to resolve the RDMA address"));
    }
    if (auto resolved = take_event(RDMA_CM_EVENT_ADDR_RESOLVED); !resolved) [[unlikely]]
    {
        return unexpected(resolved.error());
    }

    // The address decided which device this connection runs on, so this is the
    // first point the manager can be checked against it: a connection built with
    // another device's regions and keys would fail every completion.
    if (!resources_->serves(*communication_id_)) [[unlikely]]
    {
        return unexpected(std::string{ "The RDMA route resolved to a device the resource manager does not hold" });
    }

    if (::rdma_resolve_route(communication_id_, 2000) != 0) [[unlikely]]
    {
        return unexpected(Failing("Failed to resolve the RDMA route"));
    }
    if (auto resolved = take_event(RDMA_CM_EVENT_ROUTE_RESOLVED); !resolved) [[unlikely]]
    {
        return unexpected(resolved.error());
    }

    // The route is known, so the device is too: this is the earliest point the queue
    // pair can be built against it. A failure here leaves the object holding the id
    // and the channel, which its destructor gives back.
    try
    {
        build_queue_pair();
    }
    catch (const std::exception &error)
    {
        return unexpected(std::string{ error.what() });
    }

    ::rdma_conn_param param{};
    param.responder_resources = 1;
    param.initiator_depth = 1;
    param.retry_count = 7;
    param.rnr_retry_count = 7; // stall rather than fail when receives run dry
    if (::rdma_connect(communication_id_, &param) != 0) [[unlikely]]
    {
        return unexpected(Failing("Failed to connect the RDMA connection"));
    }
    if (auto established = await_established(); !established) [[unlikely]]
    {
        return unexpected(established.error());
    }
    return {};
}
} // namespace Foundation::Core

#endif // defined(__linux__)