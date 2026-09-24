#include "RdmaResourceManager.hpp"
#include "Defer.hpp"

#include <rdma/rdma_cma.h>

#include <format>
#include <utility>

namespace Foundation::Core
{
bool RdmaResourceManager::serves(const ::rdma_cm_id &id) const noexcept
{
    return device_context_ != nullptr && id.verbs == device_context_;
}
RdmaResourceManager::RdmaResourceManager(std::string_view device_name)
{
    // The contexts come from the communication manager rather than from
    // ibv_open_device: every id rdma_cm creates carries the context rdma_cm opened
    // for the device that id resolves to, and every id on one device carries the
    // same one. A protection domain, and the regions registered on it, are only
    // usable by a queue pair built on that same context, so a context opened here
    // would give a domain no queue pair could be built with.
    int num_devices{ 0 };
    ::ibv_context** device_list = ::rdma_get_devices(&num_devices);
    if (!device_list) [[unlikely]] {
        throw std::runtime_error("failed to list RDMA devices");
    }
    // Only the list is this manager's to free; the contexts in it are the
    // communication manager's, and it keeps them open for the life of the process.
    auto d1 = make_defer([&device_list] {
        ::rdma_free_devices(device_list);
    });
    if (num_devices == 0) {
        throw std::runtime_error("no RDMA devices available");
    }
    for (int i = 0; i < num_devices; i++) {
        std::string_view name = ::ibv_get_device_name(device_list[i]->device);
        if (name == device_name) {
            device_context_ = device_list[i];
            break;
        }
    }
    if (!device_context_) [[unlikely]] {
        throw std::runtime_error(
            std::format("device {} not found", device_name)
        );
    }
    protection_domain_.reset(::ibv_alloc_pd(device_context_));
    if (!protection_domain_) [[unlikely]] {
        throw std::runtime_error(
            std::format("failed to allocate protection domain for {}", device_name)
        );
    }
    // The whole block, not just its first chunk: an MR covers an address range,
    // and every chunk a connection is handed has to be inside it.
    receive_region_.reset(::ibv_reg_mr(protection_domain_.get(), receive_memory_.storage(),
                                       receive_memory_.chunk_size() * receive_memory_.capacity(),
                                       IBV_ACCESS_LOCAL_WRITE));

    if (!receive_region_) [[unlikely]] {
        throw std::runtime_error(
            std::format("failed to register receive memory region for {}", device_name)
        );
    }

    // Two-sided SEND/RECV only, so the peer has no business reaching either region
    // and neither is granted remote access.
    send_region_.reset(::ibv_reg_mr(protection_domain_.get(), send_memory_.storage(),
                                    send_memory_.chunk_size() * send_memory_.capacity(),
                                    IBV_ACCESS_LOCAL_WRITE));

    if (!send_region_) [[unlikely]] {
        throw std::runtime_error(
            std::format("failed to register send memory region for {}", device_name)
        );
    }
}

// The members do the work, and each of them knows what it holds: the regions are
// deregistered, then the protection domain is deallocated, then the device is
// closed. That is the reverse of the order they were taken in, which is what the
// member order -- declared regions last, the context first -- gets right.
RdmaResourceManager::~RdmaResourceManager() noexcept = default;
} // namespace Foundation::Core