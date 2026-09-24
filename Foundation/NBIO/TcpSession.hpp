#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/TcpSocket.hpp>
#include <atomic>
#include <memory>

#include "TcpReceiveChannel.hpp"
#include "TcpSendChannel.hpp"

namespace Foundation::NBIO
{
class TcpSession : public std::enable_shared_from_this<TcpSession>
{
  public:
    TcpSession(Foundation::Core::TcpSocket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);

    TcpSession(TcpSession &&) = delete;
    TcpSession &operator=(TcpSession &&) = delete;

    TcpSession(const TcpSession &) = delete;
    TcpSession &operator=(const TcpSession &) = delete;

    ~TcpSession() noexcept;

    // Ends the transport immediately; channel registrations are released when
    // this session is subsequently destroyed.
    void close() noexcept;

    unsigned int id() const noexcept
    {
        return id_;
    }

    // TcpSession is a thin wrapper over the transport layer: it forwards Receive/
    // Send to its simplex channels and owns the connection lifecycle.
    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> receive(std::span<char> buffer);
    Foundation::NBIO::Task<Core::expected<std::size_t, std::error_code>> send(std::span<const char> buffer);

    TcpReceiveChannel &receive_channel() noexcept
    {
        return receive_channel_;
    }
    TcpSendChannel &send_channel() noexcept
    {
        return send_channel_;
    }

    Foundation::Core::TcpSocket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::TcpSocket &socket() const noexcept
    {
        return socket_;
    }

  private:
    Foundation::Core::TcpSocket socket_;
    Foundation::NBIO::Multiplexer &multiplexer_;
    TcpReceiveChannel receive_channel_;
    TcpSendChannel send_channel_;
    unsigned int id_;
    static std::atomic_uint next_id_;
};
} // namespace Foundation::NBIO
