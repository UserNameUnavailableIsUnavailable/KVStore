#pragma once
#if defined(__linux__)

#include <rdma/rdma_cma.h>

class RdmaSendChannel
{
public:
    RdmaSendChannel();
    ~RdmaSendChannel() noexcept;
private:
    rdma_cm_id *listen_id_{ nullptr };
};

#endif // defined(__linux__)