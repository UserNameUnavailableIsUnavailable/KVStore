#pragma once

#include <Foundation/Core/RDMA_Channel.hpp>
#if defined(__linux__)

#include "RDMA_Device.hpp"
#include "Address.hpp"
#include "Native.hpp"

namespace Foundation::Core
{
class RDMA_Acceptor
{
public:
    using Handle = NativeHandle;
    RDMA_Acceptor(RDMA_Device &device, Address address);
    ~RDMA_Acceptor() noexcept;
    RDMA_Channel accept();

    void set_non_blocking(bool enabled = true);
    Handle native_handle() const noexcept { return event_channel_->fd; }    
private:
    RDMA_Device &device_;
    Address address_;
    ::rdma_event_channel *event_channel_{ nullptr };
    ::rdma_cm_id *communication_id_{ nullptr };
};
} // namespace Foundation::Core

#endif // defined(__linux__)