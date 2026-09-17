#include <Foundation/Core/BitmapMemory.hpp>
#include <utility>
#if defined(__linux__)

#include "Address.hpp"
#include "RDMA_Acceptor.hpp"

#include <fcntl.h>
#include <poll.h>
#include <rdma/rdma_cma.h>
#include <cerrno>
#include <stdexcept>

namespace Foundation::Core
{
namespace
{
enum class CmEventStatus
{
    kEvent,
    kNone,
    kFailure,
};

// Looks for an event without waiting for one, so a channel that is being polled
// can tell "nothing here yet" from "this went wrong".
CmEventStatus PollCmEvent(::rdma_event_channel *channel, ::rdma_cm_event **event) noexcept
{
    if (::rdma_get_cm_event(channel, event) == 0)
    {
        return CmEventStatus::kEvent;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
        return CmEventStatus::kNone;
    }
    return CmEventStatus::kFailure;
}

// Waits for one event. rdma_accept only starts the handshake, so the event that
// says it finished arrives a round trip later; when the accept channel owns the
// event channel it sets it non-blocking, so an immediate rdma_get_cm_event would
// report EAGAIN rather than the event that is still on its way. Wait for it
// instead of failing.
CmEventStatus WaitCmEvent(::rdma_event_channel *channel, ::rdma_cm_event **event) noexcept
{
    const int flags = ::fcntl(channel->fd, F_GETFL, 0);
    const bool non_blocking = flags >= 0 && (flags & O_NONBLOCK) != 0;
    if (!non_blocking)
    {
        return ::rdma_get_cm_event(channel, event) == 0 ? CmEventStatus::kEvent : CmEventStatus::kFailure;
    }

    while (true)
    {
        if (::rdma_get_cm_event(channel, event) == 0)
        {
            return CmEventStatus::kEvent;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            return CmEventStatus::kFailure;
        }

        pollfd waiter{.fd = channel->fd, .events = POLLIN, .revents = 0};
        if (::poll(&waiter, 1, 2000) <= 0)
        {
            errno = ETIMEDOUT;
            return CmEventStatus::kFailure;
        }
    }
}
} // namespace

RDMA_Acceptor::RDMA_Acceptor(BitmapMemory receive_pool, BitmapMemory send_pool) :
    receive_pool_(std::move(receive_pool)),
    send_pool_(std::move(send_pool))
{
    // The id is created on the event channel, so that has to exist first. Nothing
    // else can be created here: a protection domain belongs to a device context,
    // and the id only has one once it is bound.
    event_channel_ = ::rdma_create_event_channel();
    if (!event_channel_) [[unlikely]]
    {
        throw std::runtime_error("Failed to create RDMA event channel");
    }
    if (::rdma_create_id(event_channel_, &communication_id_, nullptr, RDMA_PS_TCP)) [[unlikely]]
    {
        ::rdma_destroy_event_channel(event_channel_);
        event_channel_ = nullptr;
        throw std::runtime_error("Failed to create RDMA ID");
    }
}

void RDMA_Acceptor::listen(Address address, int backlog)
{
    address_ = std::move(address);
    if (::rdma_bind_addr(communication_id_, address_.storage<::sockaddr>())) [[unlikely]]
    {
        throw std::runtime_error("Failed to bind RDMA address");
    }
    // Binding picks a device only when the address names one. A wildcard address
    // is accepted and leaves the id with no device context at all, and everything
    // below needs one: the protection domain comes from it, and ibv_alloc_pd
    // dereferences it without checking. Refuse that here, where the reason is
    // still known, rather than dying a level down.
    if (communication_id_->verbs == nullptr) [[unlikely]]
    {
        throw std::runtime_error("Failed to bind RDMA address: it names no RDMA device");
    }
    if (::rdma_listen(communication_id_, backlog)) [[unlikely]]
    {
        throw std::runtime_error("Failed to listen on RDMA address");
    }

    // Binding is what gives the id a device context, so this is the first point
    // the shared resources can exist. Accepted connections report this same
    // context, which is what lets them share this domain and these two regions.
    protection_domain_ = ::ibv_alloc_pd(communication_id_->verbs);
    if (!protection_domain_) [[unlikely]]
    {
        throw std::runtime_error("Failed to allocate RDMA protection domain");
    }

    // A receive region is written into by the HCA; a send region is only read.
    // Neither needs remote access, and granting it would let the peer reach
    // memory it has no business in.
    receive_memory_region_ =
        ::ibv_reg_mr(protection_domain_, receive_pool_.storage(),
                     receive_pool_.chunk_size() * receive_pool_.capacity(), IBV_ACCESS_LOCAL_WRITE);
    if (!receive_memory_region_) [[unlikely]]
    {
        throw std::runtime_error("Failed to register the RDMA receive region");
    }
    send_memory_region_ = ::ibv_reg_mr(protection_domain_, send_pool_.storage(),
                                       send_pool_.chunk_size() * send_pool_.capacity(), IBV_ACCESS_LOCAL_WRITE);
    if (!send_memory_region_) [[unlikely]]
    {
        throw std::runtime_error("Failed to register the RDMA send region");
    }
}

std::optional<RDMA_Stream> RDMA_Acceptor::accept()
{
    while (true)
    {
        ::rdma_cm_event *event{nullptr};
        switch (PollCmEvent(event_channel_, &event))
        {
        case CmEventStatus::kNone:
            return std::nullopt; // nothing has asked to be admitted
        case CmEventStatus::kFailure:
            throw std::runtime_error("Failed to get RDMA CM event");
        case CmEventStatus::kEvent:
            break;
        }

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST || !event->id) [[unlikely]]
        {
            // Not a new connection. Everything that happens to the connections
            // this acceptor has already admitted is reported on this same event
            // channel, and none of it is something to admit: let it go and keep
            // looking, or a replica that hangs up would stop the listener.
            ::rdma_ack_cm_event(event);
            continue;
        }

        auto *cm_id = event->id; // each connection gets an id
        if (cm_id->verbs != communication_id_->verbs) [[unlikely]]
        {
            // Another device would not match the shared domain or these regions.
            ::rdma_reject(cm_id, nullptr, 0);
            ::rdma_ack_cm_event(event);
            throw std::runtime_error("RDMA connection resolved to a different device");
        }

        // Admit only if the pools can lend this connection its chunks. Rejecting
        // early beats admitting and failing halfway, since the peer can retry.
        if (send_pool_.available() < RDMA_Stream::kSendChunks ||
            receive_pool_.available() < RDMA_Stream::kReceiveChunks) [[unlikely]]
        {
            ::rdma_reject(cm_id, nullptr, 0);
            ::rdma_ack_cm_event(event);
            throw std::runtime_error("RDMA pools cannot admit another connection");
        }

        RDMA_Stream stream(cm_id, protection_domain_, send_memory_region_->lkey, receive_memory_region_->lkey,
                           send_pool_, receive_pool_);

        ::rdma_conn_param param{};
        param.responder_resources = 1;
        param.initiator_depth = 1;
        param.retry_count = 7;
        param.rnr_retry_count = 7; // stall rather than fail when receives run dry
        if (::rdma_accept(cm_id, &param)) [[unlikely]]
        {
            ::rdma_ack_cm_event(event);
            throw std::runtime_error("Failed to accept RDMA connection");
        }
        if (::rdma_ack_cm_event(event)) [[unlikely]]
        {
            throw std::runtime_error("Failed to acknowledge RDMA CM event");
        }

        // rdma_accept is asynchronous; the queue pair is only usable once the
        // established event arrives.
        if (WaitCmEvent(event_channel_, &event) != CmEventStatus::kEvent) [[unlikely]]
        {
            throw std::runtime_error("Failed to get RDMA CM event");
        }
        const bool established = event->event == RDMA_CM_EVENT_ESTABLISHED;
        ::rdma_ack_cm_event(event);
        if (!established) [[unlikely]]
        {
            throw std::runtime_error("RDMA connection was not established");
        }

        return stream;
    }
}

void RDMA_Acceptor::set_non_blocking(bool enabled)
{
    const int flags = ::fcntl(event_channel_->fd, F_GETFL, 0);
    if (flags < 0 ||
        ::fcntl(event_channel_->fd, F_SETFL, enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) < 0)
    {
        throw std::runtime_error("Failed to set RDMA event channel non-blocking");
    }
}

RDMA_Acceptor::~RDMA_Acceptor() noexcept
{
    // Streams borrow the domain and the chunks, so they must all be gone before
    // any of this runs.
    if (send_memory_region_)
    {
        ::ibv_dereg_mr(send_memory_region_);
    }
    if (receive_memory_region_)
    {
        ::ibv_dereg_mr(receive_memory_region_);
    }
    if (protection_domain_)
    {
        ::ibv_dealloc_pd(protection_domain_);
    }
    if (communication_id_)
    {
        ::rdma_destroy_id(communication_id_);
    }
    if (event_channel_)
    {
        ::rdma_destroy_event_channel(event_channel_);
    }
}
} // namespace Foundation::Core

#endif // defined(__linux__)