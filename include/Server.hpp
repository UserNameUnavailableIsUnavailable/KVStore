#pragma once

#include <unistd.h>
#include <cstdint>
#include <array>
#include <deque>
#include <functional>
#include <unordered_map>
#include <netinet/in.h>
#include <sys/socket.h>
#include <liburing.h>

#include "CoroutineDesignator.hpp"
#include "Parser.hpp"

enum class TaskType
{
    kNone,
    kAccept,
    kRead,
    kWrite
};

struct Task
{
    TaskType type = TaskType::kNone;
    sockaddr_storage address;
    socklen_t address_length = sizeof(sockaddr_storage);
    int fd = -1;
    std::array<char, 4096> read_buffer{};
    std::string recv_buffer;
    std::deque<std::string> pending_responses;
    std::string send_buffer;
    std::size_t write_offset = 0;

    void reset()
    {
        type = TaskType::kNone;
        address_length = sizeof(address);
        if (fd >= 0)
        {
            close(fd);
        }
        fd = -1;
        recv_buffer.clear();
        pending_responses.clear();
        send_buffer.clear();
        write_offset = 0;
    }
};


namespace KV
{
class Server
{
public:
    Server();
    void Bind(std::uint16_t port);
    void Run();
    ~Server() noexcept
    {
        if (server_fd_ > 0)
        {
            close(server_fd_);
        }
    }
private:
    bool Prepare(Task& task);

    void Submit();

    void HandleTaskCompletion(io_uring_cqe& cqe);

    std::string Execute(const Request& request);

    void RegisterHandlers();

    coro::NetworkTask ExecuteCommandTask(Task& task, Request request)
    {
        task.pending_responses.push_back(Execute(request));
        co_return;
    }
private:
    using CommandHandler = std::function<std::string(const Request&)>;

    int server_fd_ = -1;
    std::uint16_t port_ = 0;
    bool bound_ = false;
    std::array<Task, 32> inprogress_;
    std::deque<std::size_t> defer_;
    coro::TaskDesignator command_designator_;
    Parser parser_;
    std::unordered_map<std::string, CommandHandler> handlers_;
    std::unordered_map<std::string, std::string> kv_;
    io_uring ring_;
};
} // namespace KV
