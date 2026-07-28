#include "Server/EpollServer.hpp"

#include <array>
#include <cerrno>
#include <format>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace KV
{
EpollServer::ReceiveAwaiter::ReceiveAwaiter(EpollServer& server, EpollSession& session) :
    server_(server), session_(session)
{
    session_.WithReceiveContext([this](int, char* data, std::size_t size) {
        data_ = data;
        size_ = size;
    });
}

bool EpollServer::ReceiveAwaiter::await_ready()
{
    result_ = Receive();
    return result_ >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK);
}

void EpollServer::ReceiveAwaiter::await_suspend(std::coroutine_handle<>)
{
    server_.RegisterWaiter(session_.GetConnection().GetFileDescriptor(), EPOLLIN);
}

ssize_t EpollServer::ReceiveAwaiter::await_resume()
{
    const ssize_t result = result_ >= 0 ? result_ : Receive();
    session_.CompleteReceive(static_cast<int>(result));
    return result;
}

ssize_t EpollServer::ReceiveAwaiter::Receive()
{
    return ::recv(session_.GetConnection().GetFileDescriptor(), data_, size_, 0);
}

EpollServer::SendAwaiter::SendAwaiter(EpollServer& server, EpollSession& session) :
    server_(server), session_(session)
{
    session_.WithSendContext([this](int, const char* data, std::size_t size) {
        data_ = data;
        size_ = size;
    });
}

bool EpollServer::SendAwaiter::await_ready()
{
    result_ = Send();
    return result_ >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK);
}

void EpollServer::SendAwaiter::await_suspend(std::coroutine_handle<>)
{
    server_.RegisterWaiter(session_.GetConnection().GetFileDescriptor(), EPOLLOUT);
}

ssize_t EpollServer::SendAwaiter::await_resume()
{
    const ssize_t result = result_ >= 0 ? result_ : Send();
    session_.CompleteSend(static_cast<int>(result));
    return result;
}

ssize_t EpollServer::SendAwaiter::Send()
{
    return ::send(session_.GetConnection().GetFileDescriptor(), data_, size_, MSG_NOSIGNAL);
}

EpollServer::~EpollServer() noexcept
{
    if (epoll_fd_ >= 0)
    {
        ::close(epoll_fd_);
    }
}

void EpollServer::Run()
{
    if (GetPort() == 0)
    {
        throw std::runtime_error("server hasn't bound to a port");
    }
    const int listener_fd = static_cast<int>(GetSocketHandle());
    if (::listen(listener_fd, SOMAXCONN) < 0)
    {
        throw std::runtime_error(std::format("failed to listen on localhost:{}, errno: {}", GetPort(), errno));
    }

    SetNonBlocking(listener_fd);
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0)
    {
        throw std::runtime_error(std::format("failed to create epoll instance, errno: {}", errno));
    }

    ::epoll_event listen_event{.events = EPOLLIN, .data = {.fd = listener_fd}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listener_fd, &listen_event) < 0)
    {
        throw std::runtime_error(std::format("failed to register listener with epoll, errno: {}", errno));
    }

    std::array<::epoll_event, 64> events{};
    while (true)
    {
        const int ready = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            throw std::runtime_error(std::format("epoll wait failed, errno: {}", errno));
        }

        for (int index = 0; index < ready; ++index)
        {
            const int fd = events[index].data.fd;
            if (fd == listener_fd)
            {
                AcceptClients();
                continue;
            }

            auto session = sessions_.find(fd);
            if (session == sessions_.end())
            {
                continue;
            }
            session->second.Resume();
            if (session->second.Done())
            {
                session->second.RethrowIfFailed();
                CloseClient(fd);
            }
        }
    }
}

EpollSession EpollServer::ServeSession(EpollSession& session)
{
    while (true)
    {
        ResolvedRequest request = session.ConsumeRequest();
        if (request.status == RequestStatus::kNeedMore)
        {
            const ssize_t received = co_await ReceiveAwaiter(*this, session);
            if (received == 0)
            {
                co_return;
            }
            if (received < 0)
            {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    continue;
                }
                co_return;
            }
            continue;
        }

        const Result result = request.status == RequestStatus::kProtocolError
            ? Result(false, std::format("protocol error: {}", request.error_message), "")
            : Execute(request.command);

        session.PrepareResponse(result);
        bool send_complete = false;
        while (!send_complete)
        {
            const ssize_t written = co_await SendAwaiter(*this, session);
            if (written < 0)
            {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    continue;
                }
                co_return;
            }
            if (written == 0)
            {
                co_return;
            }
            send_complete = session.WithSendContext([](int, const char*, std::size_t size) {
                return size == 0;
            });
        }
    }
}

void EpollServer::AcceptClients()
{
    while (true)
    {
        const int client_fd = ::accept4(GetSocketHandle(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd >= 0)
        {
            StartClient(client_fd);
            continue;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return;
        }
        throw std::runtime_error(std::format("accept failed, errno: {}", errno));
    }
}

void EpollServer::RegisterWaiter(int fd, std::uint32_t events)
{
    ::epoll_event event{.events = events | EPOLLONESHOT | EPOLLRDHUP, .data = {.fd = fd}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) < 0)
    {
        throw std::runtime_error(std::format("failed to arm client socket, errno: {}", errno));
    }
}

void EpollServer::StartClient(int fd)
{
    ::epoll_event event{.events = EPOLLONESHOT | EPOLLRDHUP, .data = {.fd = fd}};
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0)
    {
        ::close(fd);
        throw std::runtime_error(std::format("failed to register client socket, errno: {}", errno));
    }

    auto [session, inserted] = sessions_.try_emplace(fd);
    if (!inserted)
    {
        CloseClient(fd);
        return;
    }
    session->second.UseMemoryResource(GetMemoryResource());
    session->second.GetConnection().SetFileDescriptor(fd);
    session->second.AdoptCoroutine(ServeSession(session->second));
    session->second.Resume();
    if (session->second.Done())
    {
        session->second.RethrowIfFailed();
        CloseClient(fd);
    }
}

void EpollServer::CloseClient(int fd)
{
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    sessions_.erase(fd);
}

void EpollServer::SetNonBlocking(int fd) const
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        throw std::runtime_error(std::format("failed to enable nonblocking socket mode, errno: {}", errno));
    }
}
} // namespace KV