#pragma once
#if defined(__linux__)

#include <rdma/rdma_cma.h>
#include <string_view>
#include <exception>

namespace Foundation::Core
{
class RDMA_Device
{
  public:
    explicit RDMA_Device(std::size_t index = 0);
    explicit RDMA_Device(std::string_view name);
    ~RDMA_Device() noexcept;

    ::ibv_context *context() const noexcept
    {
        return context_;
    }
    ::ibv_pd *protection_domain() const noexcept
    {
        return protection_domain_;
    }

  private:
    static std::exception_ptr make(::ibv_device *device, ::ibv_context *&ctx, ::ibv_pd *&pd) noexcept;
    ::ibv_context *context_{nullptr};
    ::ibv_pd *protection_domain_{nullptr};
};
} // namespace Foundation::Core
#endif // defined(__linux__)