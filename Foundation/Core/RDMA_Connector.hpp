#pragma once
#if defined(__linux__)

#include <rdma/rdma_cma.h>

#include "Address.hpp"
#include "BitmapMemory.hpp"
#include "Native.hpp"
#include "RDMA_Stream.hpp"

namespace Foundation::Core
{
class RDMA_Connector
{
public:
    // The pools outlive the stream they lend chunks to, exactly as on the
    // acceptor side.
    RDMA_Connector(BitmapMemory receive_pool, BitmapMemory send_pool);
    RDMA_Connector(const RDMA_Connector &) = delete;
    RDMA_Connector &operator=(const RDMA_Connector &) = delete;
    ~RDMA_Connector() noexcept;

    // Resolves the peer, creates the shared domain and the two regions from the
    // id's own context, pre-posts receives and connects.
    RDMA_Stream connect(Address peer);

    std::uintptr_t native_handle() const noexcept { return event_channel_ ? event_channel_->fd : -1; }

private:
    // Waits for one event, requires it to be `expected`, and acknowledges it.
    void expect_event(::rdma_cm_event_type expected);
    void release_shared() noexcept;

    ::rdma_cm_id *communication_id_{nullptr};
    ::ibv_pd *protection_domain_{nullptr};
    ::rdma_event_channel *event_channel_{nullptr};
    BitmapMemory receive_pool_;
    BitmapMemory send_pool_;
    ibv_mr *receive_memory_region_{nullptr};
    ibv_mr *send_memory_region_{nullptr};
};
} // namespace Foundation::Core

#endif // defined(__linux__)