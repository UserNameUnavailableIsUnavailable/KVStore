#if defined(__linux__)

#include "RDMA_Channel.hpp"

#include <fcntl.h>
#include <stdexcept>
#include <utility>

namespace Foundation::Core
{
void RDMA_Channel::set_non_blocking(bool enabled)
{
    if (completion_channel_)
    {
        const int flags = ::fcntl(completion_channel_->fd, F_GETFL, 0);
        if (flags < 0 || ::fcntl(completion_channel_->fd, F_SETFL, enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) < 0)
        {
            throw std::runtime_error("Failed to set RDMA completion channel non-blocking");
        }
    }
}

RDMA_Channel::RDMA_Channel(rdma_cm_id *communication_id, ibv_pd *protection_domain) :
    communication_id_(communication_id)
{
    if (!communication_id_ || !protection_domain)
    {
        throw std::invalid_argument("RDMA channel requires a connection and protection domain");
    }

    completion_channel_ = ::ibv_create_comp_channel(communication_id_->verbs);
    if (!completion_channel_)
    {
        throw std::runtime_error("Failed to create RDMA completion channel");
    }

    try
    {
        set_non_blocking(true);

        completion_queue_ = ::ibv_create_cq(communication_id_->verbs, 16, nullptr, completion_channel_, 0);
        if (!completion_queue_)
        {
            throw std::runtime_error("Failed to create RDMA completion queue");
        }

        ibv_qp_init_attr attributes{
            .send_cq = completion_queue_,
            .recv_cq = completion_queue_,
            .cap = {
                .max_send_wr = 16,
                .max_recv_wr = 16,
                .max_send_sge = 1,
                .max_recv_sge = 1,
            },
            .qp_type = IBV_QPT_RC,
        };
        if (::rdma_create_qp(communication_id_, protection_domain, &attributes) != 0)
        {
            throw std::runtime_error("Failed to create RDMA queue pair");
        }
        queue_pair_ = communication_id_->qp;

        if (::ibv_req_notify_cq(completion_queue_, 0) != 0)
        {
            throw std::runtime_error("Failed to arm RDMA completion queue");
        }
    }
    catch (...)
    {
        if (queue_pair_)
        {
            ::rdma_destroy_qp(communication_id_);
            queue_pair_ = nullptr;
        }
        if (completion_queue_)
        {
            ::ibv_destroy_cq(completion_queue_);
            completion_queue_ = nullptr;
        }
        if (completion_channel_)
        {
            ::ibv_destroy_comp_channel(completion_channel_);
            completion_channel_ = nullptr;
        }
        throw;
    }
}

RDMA_Channel::RDMA_Channel(RDMA_Channel &&other) noexcept
    : communication_id_(std::exchange(other.communication_id_, nullptr)),
      completion_channel_(std::exchange(other.completion_channel_, nullptr)),
      completion_queue_(std::exchange(other.completion_queue_, nullptr)),
      queue_pair_(std::exchange(other.queue_pair_, nullptr))
{
}

RDMA_Channel &RDMA_Channel::operator=(RDMA_Channel &&other) noexcept
{
    if (this != &other)
    {
        reset();
        communication_id_ = std::exchange(other.communication_id_, nullptr);
        completion_channel_ = std::exchange(other.completion_channel_, nullptr);
        completion_queue_ = std::exchange(other.completion_queue_, nullptr);
        queue_pair_ = std::exchange(other.queue_pair_, nullptr);
    }
    return *this;
}

RDMA_Channel::~RDMA_Channel() noexcept
{
    reset();
}

void RDMA_Channel::reset() noexcept
{
    if (queue_pair_)
    {
        ::rdma_destroy_qp(communication_id_);
    }
    if (completion_queue_)
    {
        ::ibv_destroy_cq(completion_queue_);
    }
    if (completion_channel_)
    {
        ::ibv_destroy_comp_channel(completion_channel_);
    }
    if (communication_id_)
    {
        ::rdma_destroy_id(communication_id_);
    }
    queue_pair_ = nullptr;
    completion_queue_ = nullptr;
    completion_channel_ = nullptr;
    communication_id_ = nullptr;
}

int RDMA_Channel::poll(ibv_wc *work_completions, int capacity) noexcept
{
    if (!completion_queue_ || !work_completions || capacity <= 0)
    {
        return 0;
    }
    return ::ibv_poll_cq(completion_queue_, capacity, work_completions);
}
} // namespace Foundation::Core

#endif // defined(__linux__)