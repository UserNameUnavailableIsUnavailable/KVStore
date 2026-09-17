#if defined(__linux__)

#include "RDMA_Connector.hpp"

#include <stdexcept>
#include <string>
#include <utility>

namespace Foundation::Core
{
RDMA_Connector::RDMA_Connector(BitmapMemory receive_pool, BitmapMemory send_pool) :
    receive_pool_(std::move(receive_pool)),
    send_pool_(std::move(send_pool))
{
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

void RDMA_Connector::release_shared() noexcept
{
    if (send_memory_region_)
    {
        ::ibv_dereg_mr(send_memory_region_);
        send_memory_region_ = nullptr;
    }
    if (receive_memory_region_)
    {
        ::ibv_dereg_mr(receive_memory_region_);
        receive_memory_region_ = nullptr;
    }
    if (protection_domain_)
    {
        ::ibv_dealloc_pd(protection_domain_);
        protection_domain_ = nullptr;
    }
}

void RDMA_Connector::expect_event(::rdma_cm_event_type expected)
{
    ::rdma_cm_event *event{nullptr};
    if (::rdma_get_cm_event(event_channel_, &event)) [[unlikely]]
    {
        throw std::runtime_error("Failed to get RDMA CM event");
    }
    const auto received = event->event;
    ::rdma_ack_cm_event(event);
    if (received != expected) [[unlikely]]
    {
        throw std::runtime_error(std::string{"Unexpected RDMA CM event: "} + ::rdma_event_str(received));
    }
}

RDMA_Stream RDMA_Connector::connect(Address peer)
{
    try
    {
        // Address resolution binds the id to a local device, and only then does
        // it have a device context to build the domain and regions on.
        if (::rdma_resolve_addr(communication_id_, nullptr, const_cast<::sockaddr *>(peer.storage<::sockaddr>()),
                                2000)) [[unlikely]]
        {
            throw std::runtime_error("Failed to resolve RDMA address");
        }
        expect_event(RDMA_CM_EVENT_ADDR_RESOLVED);

        protection_domain_ = ::ibv_alloc_pd(communication_id_->verbs);
        if (!protection_domain_) [[unlikely]]
        {
            throw std::runtime_error("Failed to allocate RDMA protection domain");
        }
        receive_memory_region_ =
            ::ibv_reg_mr(protection_domain_, receive_pool_.storage(),
                         receive_pool_.chunk_size() * receive_pool_.capacity(), IBV_ACCESS_LOCAL_WRITE);
        if (!receive_memory_region_) [[unlikely]]
        {
            throw std::runtime_error("Failed to register the RDMA receive region");
        }
        send_memory_region_ =
            ::ibv_reg_mr(protection_domain_, send_pool_.storage(),
                         send_pool_.chunk_size() * send_pool_.capacity(), IBV_ACCESS_LOCAL_WRITE);
        if (!send_memory_region_) [[unlikely]]
        {
            throw std::runtime_error("Failed to register the RDMA send region");
        }

        if (::rdma_resolve_route(communication_id_, 2000)) [[unlikely]]
        {
            throw std::runtime_error("Failed to resolve RDMA route");
        }
        expect_event(RDMA_CM_EVENT_ROUTE_RESOLVED);

        if (send_pool_.available() < RDMA_Stream::kSendChunks ||
            receive_pool_.available() < RDMA_Stream::kReceiveChunks) [[unlikely]]
        {
            throw std::runtime_error("RDMA pools cannot serve another connection");
        }

        RDMA_Stream stream(communication_id_, protection_domain_, send_memory_region_->lkey,
                           receive_memory_region_->lkey, send_pool_, receive_pool_);
        communication_id_ = nullptr; // the stream owns it now

        ::rdma_conn_param param{};
        param.responder_resources = 1;
        param.initiator_depth = 1;
        param.retry_count = 7;
        param.rnr_retry_count = 7; // stall rather than fail when receives run dry
        if (::rdma_connect(stream.communication_id_, &param)) [[unlikely]]
        {
            throw std::runtime_error("Failed to connect RDMA channel");
        }
        expect_event(RDMA_CM_EVENT_ESTABLISHED);

        return stream;
    }
    catch (...)
    {
        // `communication_id_` is null once the stream owns it, so this only
        // frees an id the connection never handed over.
        if (communication_id_)
        {
            ::rdma_destroy_id(communication_id_);
            communication_id_ = nullptr;
        }
        release_shared();
        throw;
    }
}

RDMA_Connector::~RDMA_Connector() noexcept
{
    if (communication_id_)
    {
        ::rdma_destroy_id(communication_id_);
    }
    release_shared();
    if (event_channel_)
    {
        ::rdma_destroy_event_channel(event_channel_);
    }
}
} // namespace Foundation::Core

#endif // defined(__linux__)
