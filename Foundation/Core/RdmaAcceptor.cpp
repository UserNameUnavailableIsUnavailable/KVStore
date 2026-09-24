#include <utility>
#if defined(__linux__)

#include "RdmaAcceptor.hpp"
#include "SocketAddress.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <rdma/rdma_cma.h>
#include <stdexcept>
#include <string>
#include <string_view>

namespace Foundation::Core
{
namespace
{
// The verbs and rdma_cm calls report a failure as a nonzero return plus errno,
// so every message this file builds ends the same way.
std::string Failing(std::string_view what)
{
    return std::string{ what } + ": " + ::strerror(errno);
}

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
} // namespace

RdmaAcceptor::RdmaAcceptor(RdmaResourceManager &resources) : resources_(&resources)
{
    // The id is created on the event channel, so that has to exist first. Nothing
    // else is created here: what a connection is built from comes from the manager,
    // and it is only needed once one asks to be admitted.
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

expected<void, std::string> RdmaAcceptor::listen(SocketAddress address, int backlog) noexcept
{
    address_ = std::move(address);
    if (::rdma_bind_addr(communication_id_, address_.storage<::sockaddr>())) [[unlikely]]
    {
        return unexpected(Failing("Failed to bind RDMA address"));
    }
    // The address is what picks the device, and the manager holds one: an address
    // that resolves somewhere else would give every connection it admits a queue
    // pair built with regions and keys that device does not know.
    if (!resources_->serves(*communication_id_)) [[unlikely]]
    {
        return unexpected(std::string{ "Failed to bind RDMA address: it does not name the manager's device" });
    }
    if (::rdma_listen(communication_id_, backlog)) [[unlikely]]
    {
        return unexpected(Failing("Failed to listen on RDMA address"));
    }
    return {};
}

expected<std::optional<RdmaConnector>, std::string> RdmaAcceptor::accept() noexcept
{
    while (true)
    {
        ::rdma_cm_event *event{nullptr};
        switch (PollCmEvent(event_channel_, &event))
        {
        case CmEventStatus::kNone:
            return std::optional<RdmaConnector>{}; // nothing has asked to be admitted
        case CmEventStatus::kFailure:
            return unexpected(Failing("Failed to get RDMA CM event"));
        case CmEventStatus::kEvent:
            break;
        }

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST || !event->id) [[unlikely]]
        {
            // The listener's channel carries what happens to the listener -- a
            // device going away, the listener being taken down -- and nothing of it
            // is a connection to admit. A connection's own events are on the
            // channel it was migrated to, so none of them can turn up here.
            ::rdma_ack_cm_event(event);
            continue;
        }

        auto *cm_id = event->id; // the kernel makes one id per connection
        if (!resources_->serves(*cm_id)) [[unlikely]]
        {
            ::rdma_reject(cm_id, nullptr, 0);
            ::rdma_ack_cm_event(event);
            return unexpected(std::string{ "RDMA connection resolved to another device than the manager holds" });
        }

        // An acceptor gets a new connection request on the event channel, then it creates a new channel for that connection, moving the connector's id to that event channel.
        ::rdma_event_channel *channel = ::rdma_create_event_channel();
        if (!channel) [[unlikely]]
        {
            ::rdma_reject(cm_id, nullptr, 0);
            ::rdma_ack_cm_event(event);
            return unexpected(Failing("Failed to create an RDMA event channel for a connection"));
        }
        if (::rdma_ack_cm_event(event) != 0) [[unlikely]]
        {
            ::rdma_destroy_event_channel(channel);
            ::rdma_reject(cm_id, nullptr, 0);
            return unexpected(Failing("Failed to acknowledge RDMA CM event"));
        }
        // move the cm_id to the connector's channel for load balancing
        if (::rdma_migrate_id(cm_id, channel) != 0) [[unlikely]]
        {
            ::rdma_destroy_event_channel(channel);
            ::rdma_reject(cm_id, nullptr, 0);
            return unexpected(Failing("Failed to migrate an RDMA connection to its own event channel"));
        }

        // Building the connection is the one step that can still throw: it creates
        // two completion queues, a queue pair and takes its chunks. Whatever it
        // cannot get it gives back itself, so all that is left to do here is to
        // reject the connection and report why.
        std::optional<RdmaConnector> connection;
        try
        {
            connection = RdmaConnector(cm_id, channel, *resources_);
        }
        catch (const std::exception &error)
        {
            // The connection never took either of them.
            ::rdma_reject(cm_id, nullptr, 0);
            ::rdma_destroy_id(cm_id);
            ::rdma_destroy_event_channel(channel);
            return unexpected(std::string{ error.what() });
        }

        ::rdma_conn_param param{};
        param.responder_resources = 1;
        param.initiator_depth = 1;
        param.retry_count = 7;
        param.rnr_retry_count = 7; // stall rather than fail when receives run dry
        if (::rdma_accept(cm_id, &param)) [[unlikely]]
        {
            return unexpected(Failing("Failed to accept RDMA connection"));
        }

        // rdma_accept only starts the handshake: the connection is usable once it
        // says so, and that event arrives on the connection's own channel. A
        // failure here tears the connection down through its destructor.
        if (auto established = connection->await_established(); !established) [[unlikely]]
        {
            return unexpected(established.error());
        }
        return std::move(connection);
    }
}

expected<void, std::string> RdmaAcceptor::non_blocking(bool toggle) noexcept
{
    const int flags = ::fcntl(event_channel_->fd, F_GETFL, 0);
    if (flags < 0 ||
        ::fcntl(event_channel_->fd, F_SETFL, toggle ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) < 0) [[unlikely]]
    {
        return unexpected(Failing("Failed to set the RDMA event channel non-blocking"));
    }
    return {};
}

RdmaAcceptor::~RdmaAcceptor() noexcept
{
    // Connections borrow the manager's device, domain and chunks, so they are all
    // gone before it is: this listener gives back its own id and its own channel.
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