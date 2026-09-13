#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <atomic>
#include <memory>

#include "ReceiveChannel.hpp"
#include "SendChannel.hpp"

namespace Foundation::NBIO
{
class Session : protected std::enable_shared_from_this<Session>
{
  public:
    Session(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);

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
    Foundation::NBIO::Task<Foundation::Core::ReceiveResult> receive(Foundation::Core::Buffer &buffer);
    Foundation::NBIO::Task<Foundation::Core::SendResult> send(Foundation::Core::Buffer &buffer);

    ReceiveChannel &receive_channel() noexcept
    {
        return receive_channel_;
    }
    SendChannel &send_channel() noexcept
    {
        return send_channel_;
    }

    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }

  private:
    Foundation::Core::Socket socket_;
    Foundation::NBIO::Multiplexer &multiplexer_;
    ReceiveChannel receive_channel_;
    SendChannel send_channel_;
    unsigned int id_;
    static std::atomic_uint next_id_;
};
} // namespace Foundation::NBIO
