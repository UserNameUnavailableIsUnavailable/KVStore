#pragma once
#if defined(__linux__)

#include <infiniband/verbs.h>
#include "BitmapMemory.hpp"

#include <memory>
#include <stdexcept>
#include <string_view>

// The RDMA CM id is only ever taken by reference here, so its definition is not
// needed: what an id carries is the device, and the device is what is compared.
struct rdma_cm_id;

namespace Foundation::Core
{
// This class is designed to be used in a single thread
class RdmaResourceManager
{
public:
    RdmaResourceManager(std::string_view device_name);
    ~RdmaResourceManager() noexcept;

    RdmaResourceManager(const RdmaResourceManager &) = delete;
    RdmaResourceManager &operator=(const RdmaResourceManager &) = delete;
    
    ::ibv_context* device_context() noexcept { return device_context_; }
    ::ibv_pd* protection_domain() noexcept { return protection_domain_.get(); }
    ::ibv_mr* receive_region() noexcept { return receive_region_.get(); }
    ::ibv_mr* send_region() noexcept { return send_region_.get(); }

    // Whether a connection that rdma_cm has resolved for this id would run on the
    // device this manager holds. Every id on one device carries the same context,
    // so this is what says the id's queue pair would be built on the context this
    // manager's domain and regions were made on -- a connection built with another
    // device's regions and keys fails every completion it posts.
    bool serves(const ::rdma_cm_id &id) const noexcept;

    BitmapMemory& receive_memory()
    {
        return receive_memory_;
    }
    BitmapMemory& send_memory()
    {
        return send_memory_;
    }

private:
    struct PdDel {
        void operator()(::ibv_pd* pd) noexcept {
            if (pd) {
                ::ibv_dealloc_pd(pd);
            }
        }
    };
    struct MrDel {
        void operator()(::ibv_mr* mr) noexcept {
            if (mr) {
                ::ibv_dereg_mr(mr);
            }
        }
    };

    // Borrowed from the communication manager, never closed here: it is the
    // context every id on this device carries, so it is the one the queue pairs are
    // built on, and the manager that owns it outlives every connection.
    ::ibv_context* device_context_{nullptr};
    std::unique_ptr<::ibv_pd, PdDel> protection_domain_;
    BitmapMemory receive_memory_{4096, 4096};
    BitmapMemory send_memory_{4096, 4096};
    std::unique_ptr<::ibv_mr, MrDel> receive_region_;
    std::unique_ptr<::ibv_mr, MrDel> send_region_;
};
} // namespace Foundation::Core

#endif