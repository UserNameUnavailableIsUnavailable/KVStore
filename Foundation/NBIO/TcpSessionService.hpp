#pragma once

#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/TcpConnector.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "TcpReceiveChannel.hpp"
#include "TcpSendChannel.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <span>
#include <system_error>
#include <utility>

namespace Foundation::NBIO
{
// A connection that is up, with the two simplex channels that carry it: what both
// services hand back once a connection has been made -- the accepting one with the
// address it came from, the connecting one without, because that end is the one that
// chose it.
//
// The connector is taken over rather than pointed at: whoever made the connection
// gives it up here, and the channels are built on it. The service that made it is
// spent, and what is left is one object that owns the connection and can carry bytes
// both ways.
//
// A session cannot be copied or moved -- it owns two channels, and a channel is never
// moved because the multiplexer holds a pointer to it -- so it is handed around as a
// `shared_ptr`, which is what both services answer with.
class TcpSessionService final : public std::enable_shared_from_this<TcpSessionService>
{
  public:
    // Attached to the engine installed on this thread, which is where the connection
    // it is given will be read and written from.
    explicit TcpSessionService(Foundation::Core::TcpConnector connector);

    TcpSessionService(const TcpSessionService &) = delete;
    TcpSessionService &operator=(const TcpSessionService &) = delete;
    TcpSessionService(TcpSessionService &&) = delete;
    TcpSessionService &operator=(TcpSessionService &&) = delete;

    ~TcpSessionService() noexcept;

    // Ends the transport immediately; the channels unregister themselves when this
    // session is destroyed.
    void close() noexcept;

    unsigned int id() const noexcept
    {
        return id_;
    }

    // TcpSessionService is a thin wrapper over the transport: it forwards receive and
    // send to its simplex channels and owns the connection's lifetime.
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

    Foundation::Core::TcpConnector &connector() noexcept
    {
        return connector_;
    }

    const Foundation::Core::TcpConnector &connector() const noexcept
    {
        return connector_;
    }

  private:
    // Declared first: the channels are built on this connection, so it has to outlive
    // them, and the member order is what says so.
    Foundation::Core::TcpConnector connector_;
    TcpReceiveChannel receive_channel_;
    TcpSendChannel send_channel_;
    unsigned int id_;
    static std::atomic_uint next_id_;
};
} // namespace Foundation::NBIO
