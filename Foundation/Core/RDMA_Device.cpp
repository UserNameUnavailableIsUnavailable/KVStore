#if defined(__linux__)

#include "RDMA_Device.hpp"
#include "Defer.hpp"

#include <exception>
#include <stdexcept>

namespace Foundation::Core
{
RDMA_Device::RDMA_Device(std::size_t index)
{
    int num;
    bool error_raised{false};
    ibv_device **devices{nullptr};

    // Defer cleanup of RDMA resources in case of an error.
    // We create defer object before acquiring any RDMA resources since
    // the creation of the defer object can fail, though very unlikely.
    make_defer([&error_raised, &devices, this] {
        if (devices)
        {
            ::ibv_free_device_list(devices);
        }
        if (error_raised)
        {
            if (context_)
            {
                ::ibv_close_device(context_);
            }
            if (protection_domain_)
            {
                ::ibv_dealloc_pd(protection_domain_);
            }
        }
    });

    devices = ::ibv_get_device_list(&num);
    if (!devices) [[unlikely]]
    {
        error_raised = true;
        throw std::runtime_error("Failed to get RDMA device list");
    }
    if (num < 0)
    {
        error_raised = true;
        throw std::runtime_error("no RDMA devices found");
    }
    if (index >= static_cast<std::size_t>(num)) [[unlikely]]
    {
        error_raised = true;
        throw std::runtime_error("RDMA device index out of range");
    }
    auto exception = make(devices[index], context_, protection_domain_);
    if (exception)
    {
        error_raised = true;
        std::rethrow_exception(exception);
    }
}

RDMA_Device::RDMA_Device(std::string_view name)
{
    int num;
    bool error_raised{false};
    ::ibv_device **devices{nullptr};

    // Defer cleanup of RDMA resources in case of an error.
    // We create defer object before acquiring any RDMA resources since
    // the creation of the defer object can fail, though very unlikely.
    make_defer([&error_raised, &devices, this] {
        if (devices)
        {
            ::ibv_free_device_list(devices);
        }
        if (error_raised)
        {
            if (context_)
            {
                ::ibv_close_device(context_);
            }
            if (protection_domain_)
            {
                ::ibv_dealloc_pd(protection_domain_);
            }
        }
    });

    devices = ::ibv_get_device_list(&num);
    if (!devices) [[unlikely]]
    {
        error_raised = true;
        throw std::runtime_error("Failed to get RDMA device list");
    }
    if (num < 0)
    {
        error_raised = true;
        throw std::runtime_error("no RDMA devices found");
    }
    ibv_device *device = nullptr;
    for (int i = 0; i < num; ++i)
    {
        if (name == ::ibv_get_device_name(devices[i]))
        {
            device = devices[i];
            break;
        }
    }
    if (!device) [[unlikely]]
    {
        error_raised = true;
        throw std::runtime_error("RDMA device not found");
    }
    auto exception = make(device, context_, protection_domain_);
    if (exception)
    {
        error_raised = true;
        std::rethrow_exception(exception);
    }
}

std::exception_ptr RDMA_Device::make(ibv_device *device, ibv_context *&ctx, ibv_pd *&pd) noexcept
{
    ctx = ::ibv_open_device(device);
    if (!ctx) [[unlikely]]
    {
        return std::make_exception_ptr(std::runtime_error("Failed to open RDMA device"));
    }
    pd = ::ibv_alloc_pd(ctx);
    if (!pd) [[unlikely]]
    {
        return std::make_exception_ptr(std::runtime_error("Failed to allocate protection domain"));
    }
    return nullptr;
}

RDMA_Device::~RDMA_Device() noexcept
{
    if (protection_domain_)
    {
        ::ibv_dealloc_pd(protection_domain_);
    }
    if (context_)
    {
        ::ibv_close_device(context_);
    }
}
} // namespace Foundation::Core

#endif // defined(__linux__)
