#pragma once

#include <array>
#include <cstddef>
#include <deque>

#include <liburing.h>

#include "Server/IOUringSession.hpp"
#include "Server/Server.hpp"

namespace KV
{
class IOUringServer final : public Server
{
public:
    void Run() override;
    ~IOUringServer() noexcept override;

private:
    bool PrepareTask(IOUringSession& session);
    void SubmitTasks();
    void HandleTaskCompletion(io_uring_cqe& cqe);
    void InitializeSubmissionQueue();

    std::array<IOUringSession, 1024> sessions_;
    std::deque<std::size_t> defer_;
    io_uring ring_{};
    bool ring_initialized_ = false;
    std::size_t submission_queue_capacity_ = 1024;
    std::size_t completion_queue_capacity_ = 2048;
};
} // namespace KV