#pragma once

#include <unistd.h>
#include <cstdint>
#include <array>
#include <deque>
#include <functional>
#include <string>
#include <unordered_map>
#include <netinet/in.h>
#include <sys/socket.h>
#include <liburing.h>

#include "Common/Task.hpp"
#include "CoroutineDesignator.hpp"
#include "Common/Command.hpp"
#include "Common/LRUCache.hpp"
#include "Common/Result.hpp"

namespace KV
{
class Server
{
public:
    Server();
    void Bind(std::uint16_t port);
    void Run();
    ~Server() noexcept;
private:
    /// Prepares a task's current I/O operation for submission. Returns false when the submission queue is full and the task is deferred.
    bool PrepareTask(IOTask& task);
    void SubmitTasks();
    void HandleTaskCompletion(io_uring_cqe& cqe);
    Result Execute(const Command& command);
    void RegisterCommaandHandler(std::string command_name, std::function<Result (const Command& command, LRUCache<std::string>& cache)> handler);
    void RegisterHandlers();
    coro::NetworkTask ExecuteCommandTask(IOTask& task, Command command)
    {
        const auto response = Execute(command).Serialize();
        task.WithMutableWriteBuffer([&response](auto& buffer) {
            buffer.assign(response.begin(), response.end());
        });
        co_return;
    }
private:
    using CommandHandler = std::function<Result(const Command&)>;
    void InitializeSubmissionQueue();
    int server_fd_ = -1;
    std::uint16_t port_ = 0;
    std::array<IOTask, 1024> io_tasks_;
    std::deque<std::size_t> defer_;
    coro::TaskDesignator command_designator_;
    std::unordered_map<std::string, CommandHandler> handlers_;
	LRUCache<std::string> lru_cache_;
    io_uring ring_;
    bool ring_initialized_ = false;
    std::size_t submission_queue_capacity_ = 1024; // max number of entries in the submission queue
    std::size_t completion_queue_capacity_ = 2048; // max number of entries in the completion queue (2 times the submission queue capacity)
};
} // namespace KV
