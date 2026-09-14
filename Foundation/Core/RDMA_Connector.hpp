#pragma once
#if defined(__linux__)

#include <rdma/rdma_cma.h>

namespace Foundation::Core
{
class RDMA_Connector
{
public:
    RDMA_Connector();
    ~RDMA_Connector() noexcept;

private:
    ibv_wc *work_completion_{ nullptr };
};
} // namespace Foundation::Core

#endif // defined(__linux__)