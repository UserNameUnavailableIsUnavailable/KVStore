#pragma once
#if defined(__linux__)

#include <rdma/rdma_cma.h>

class RdmaListenChannel
{
public:
    RdmaListenChannel();
    ~RdmaListenChannel() noexcept;
private:
    ibv_wc *work_completion_{ nullptr };
};

#endif // defined(__linux__)