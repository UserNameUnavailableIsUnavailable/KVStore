#pragma once

#include <coroutine>
#include <cstdint>
#include <unordered_map>

#include "Server/EpollSession.hpp"
#include "Server/Server.hpp"

namespace KV
{
class EpollServer final : public Server
{
public:
    EpollServer() = default;
    ~EpollServer() noexcept override;

    void Run() override;

private:
    class ReceiveAwaiter
    {
    public:
        ReceiveAwaiter(EpollServer& server, EpollSession& session);
        bool await_ready();
        void await_suspend(std::coroutine_handle<>);
        ssize_t await_resume();

    private:
        ssize_t Receive();

        EpollServer& server_;
        EpollSession& session_;
        char* data_ = nullptr;
        std::size_t size_ = 0;
        ssize_t result_ = -1;
    };

    class SendAwaiter
    {
    public:
        SendAwaiter(EpollServer& server, EpollSession& session);
        bool await_ready();
        void await_suspend(std::coroutine_handle<>);
        ssize_t await_resume();

    private:
        ssize_t Send();

        EpollServer& server_;
        EpollSession& session_;
        const char* data_ = nullptr;
        std::size_t size_ = 0;
        ssize_t result_ = -1;
    };

    EpollSession ServeSession(EpollSession& session);
    void AcceptClients();
    void RegisterWaiter(int fd, std::uint32_t events);
    void StartClient(int fd);
    void CloseClient(int fd);
    void SetNonBlocking(int fd) const;

    int epoll_fd_ = -1;
    std::unordered_map<int, EpollSession> sessions_;
};
} // namespace KV