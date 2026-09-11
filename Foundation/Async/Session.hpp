#pragma once

#include <Foundation/Buffer.hpp>
#include <Foundation/Socket.hpp>
#include <atomic>
#include <memory>

#include "Multiplexer.hpp"
#include "ReceiveChannel.hpp"
#include "Scheduler.hpp"
#include "SendChannel.hpp"
#include "Task.hpp"

namespace Foundation::Async
{
class Session : protected std::enable_shared_from_this<Session>
{
  public:
    Session(Foundation::Socket socket, Multiplexer &multiplexer, Scheduler &scheduler);

    Session(Session &&) = delete;
    Session &operator=(Session &&) = delete;

    Session(const Session &) = delete;
    Session &operator=(const Session &) = delete;

    ~Session() noexcept;

    // Ends the transport immediately; channel registrations are released when
    // this session is subsequently destroyed.
    void close() noexcept;

    unsigned int id() const noexcept
    {
        return id_;
    }

    // Session is a thin wrapper over the transport layer: it forwards Receive/
    // Send to its simplex channels and owns the connection lifecycle.
    Task<ReceiveResult> receive(Buffer &buffer);
    Task<SendResult> send(Buffer &buffer);

    ReceiveChannel &receive_channel() noexcept
    {
        return receive_channel_;
    }
    SendChannel &send_channel() noexcept
    {
        return send_channel_;
    }

    Socket &socket() noexcept
    {
        return socket_;
    }
    const Socket &socket() const noexcept
    {
        return socket_;
    }

  private:
    // destruction order: channels are destroyed before socket (closes fd)
    Foundation::Socket socket_; // session owns socket
    Multiplexer &multiplexer_;
    ReceiveChannel receive_channel_;
    SendChannel send_channel_;
    unsigned int id_;
    static std::atomic_uint next_id_;
};
} // namespace Foundation::Async
