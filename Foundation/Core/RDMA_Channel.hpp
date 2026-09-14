#pragma once
#if defined(__linux__)

#include <rdma/rdma_cma.h>

#include "Native.hpp"

namespace Foundation::Core
{
class RDMA_Acceptor;
class RDMA_Connector;

class RDMA_Channel
{
friend Foundation::Core::RDMA_Acceptor;
friend Foundation::Core::RDMA_Connector;
public:
    using Handle = NativeHandle;

    RDMA_Channel(const RDMA_Channel &) = delete;
    RDMA_Channel &operator=(const RDMA_Channel &) = delete;
    RDMA_Channel(RDMA_Channel &&other) noexcept;
    RDMA_Channel &operator=(RDMA_Channel &&other) noexcept;
    ~RDMA_Channel() noexcept;

    Handle native_handle() const noexcept
    {
        return completion_channel_ ? completion_channel_->fd : -1;
    }

    rdma_cm_id *communication_id() const noexcept
    {
        return communication_id_;
    }

    ibv_cq *completion_queue() const noexcept
    {
        return completion_queue_;
    }

    ibv_qp *queue_pair() const noexcept
    {
        return queue_pair_;
    }

    void set_non_blocking(bool enabled = true);

    int poll(struct ibv_wc *work_completions, int capacity) noexcept;

private:
    RDMA_Channel(rdma_cm_id *communication_id, ibv_pd *protection_domain);
    void reset() noexcept;

    rdma_cm_id *communication_id_{ nullptr };
    ibv_comp_channel *completion_channel_{ nullptr };
    ibv_cq *completion_queue_{ nullptr };
    ibv_qp *queue_pair_{ nullptr };
};
} // namespace Foundation::Core


#endif // defined(__linux__)