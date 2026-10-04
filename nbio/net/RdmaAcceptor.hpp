#pragma once

#include <nbio/utility/Expected.hpp>
#include <nbio/net/RdmaConnector.hpp>
#include <nbio/net/RdmaResourceManager.hpp>

#if defined(__linux__)

#include <cstdint>
#include <optional>
#include <string>

#include <nbio/net/Address.hpp>

namespace nbio::net {
// The listening end of an rdma link, and nothing else: the device, the protection
// domain, the regions and the pools a connection is built from belong to the
// resource manager it is given, and the connections themselves outlive it.
class RdmaAcceptor {
   public:
    explicit RdmaAcceptor(RdmaResourceManager& resources);
    ~RdmaAcceptor() noexcept;

    RdmaAcceptor(const RdmaAcceptor&) = delete;
    RdmaAcceptor& operator=(const RdmaAcceptor&) = delete;

    utility::expected<void, std::string> listen(net::Address address, int backlog = 4096) noexcept;

    // A connection that has finished its handshake, or nothing when nobody has
    // asked to be admitted.
    utility::expected<std::optional<RdmaConnector>, std::string> Accept() noexcept;

    utility::expected<void, std::string> NonBlocking(bool enabled = true) noexcept;
    utility::expected<void, std::string> ReuseAddress(bool enabled = true) noexcept;

    std::uintptr_t native_handle() const noexcept {
        return event_channel_ ? static_cast<std::uintptr_t>(event_channel_->fd) : static_cast<std::uintptr_t>(-1);
    }

   private:
    RdmaResourceManager* resources_{nullptr};
    ::rdma_cm_id* communication_id_{nullptr};  // the listener itself, never a connection
    ::rdma_event_channel* event_channel_{nullptr};
    net::Address address_;
};
}  // namespace nbio::net

#endif  // defined(__linux__)

