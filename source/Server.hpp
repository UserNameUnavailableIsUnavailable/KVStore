#pragma once

#include <unistd.h>
#include <stdexcept>
#include <format>
#include <array>
#include <deque>
#include <unordered_map>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <liburing.h>
#include <iostream>

#include "CoroutineDesignator.hpp"

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

    std::string Execute(std::string request);

    coro::NetworkTask ExecuteCommandTask(Task& task, std::string line)
    {
        task.pending_responses.push_back(Execute(std::move(line)));
        co_return;
    }
private:
    int server_fd_ = -1;
    std::uint16_t port_ = 0;
    bool bound_ = false;
    std::array<Task, 32> inprogress_;
    std::deque<std::size_t> defer_;
    coro::TaskDesignator command_designator_;
    std::unordered_map<std::string, std::string> kv_;
    io_uring ring_;
};
} // namespace KV
