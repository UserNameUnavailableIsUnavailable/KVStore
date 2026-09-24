#pragma once

#include <Foundation/Core/Expected.hpp>
#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/Core/RdmaResourceManager.hpp>

#if defined(__linux__)

#include <cstdint>
#include <optional>
#include <string>

#include "SocketAddress.hpp"

namespace Foundation::Core
{
// The listening end of an RDMA link, and nothing else: the device, the protection
// domain, the regions and the pools a connection is built from belong to the
// resource manager it is given, and the connections themselves outlive it.
class RdmaAcceptor
{
public:
    explicit RdmaAcceptor(RdmaResourceManager &resources);
    ~RdmaAcceptor() noexcept;

    RdmaAcceptor(const RdmaAcceptor &) = delete;
    RdmaAcceptor &operator=(const RdmaAcceptor &) = delete;

    expected<void, std::string> listen(SocketAddress address, int backlog = 4096) noexcept;

    // A connection that has finished its handshake, or nothing when nobody has
    // asked to be admitted.
    expected<std::optional<RdmaConnector>, std::string> accept() noexcept;

    expected<void, std::string> non_blocking(bool enabled = true) noexcept;
    std::uintptr_t native_handle() const noexcept
    {
        return event_channel_ ? static_cast<std::uintptr_t>(event_channel_->fd) : static_cast<std::uintptr_t>(-1);
    }

private:
    RdmaResourceManager *resources_{ nullptr };
    ::rdma_cm_id *communication_id_{ nullptr }; // the listener itself, never a connection
    ::rdma_event_channel *event_channel_{ nullptr };
    SocketAddress address_;
};
} // namespace Foundation::Core

#endif // defined(__linux__)