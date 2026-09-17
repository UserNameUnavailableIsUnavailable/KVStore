#pragma once

#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/RDMA_Stream.hpp>
#include <infiniband/verbs.h>

#if defined(__linux__)

#include <optional>

#include "Address.hpp"
#include "Native.hpp"

namespace Foundation::Core
{
class RDMA_Acceptor
{
public:
    using Handle = NativeHandle;
    explicit RDMA_Acceptor(BitmapMemory receive_pool, BitmapMemory send_pool);
    ~RDMA_Acceptor() noexcept;

    void listen(Address address, int backlog = 4096);

    // A connection asking to be admitted, or nothing when none is waiting right
    // now -- which is not a failure, it is a channel with nothing on it yet.
    //
    // This channel also carries the events of the connections it has already
    // admitted, a disconnect for instance, and those are consumed here rather
    // than mistaken for something to accept.
    std::optional<RDMA_Stream> accept();

    void set_non_blocking(bool enabled = true);
    Handle native_handle() const noexcept { return event_channel_->fd; }

private:
    ::rdma_cm_id *communication_id_{ nullptr };
    ::ibv_pd *protection_domain_{ nullptr };
    ::rdma_event_channel *event_channel_{ nullptr };
    // Owned here; the streams borrow chunks from these for their lifetime.
    BitmapMemory receive_pool_;
    BitmapMemory send_pool_;
    ibv_mr *receive_memory_region_{ nullptr };
    ibv_mr *send_memory_region_{ nullptr };
    
    Address address_;
};
} // namespace Foundation::Core

#endif // defined(__linux__)