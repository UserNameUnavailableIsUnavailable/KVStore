#include "TcpSessionService.hpp"

#include <nbio/runtime/Runtime.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <atomic>
#include <utility>

namespace nbio::net {
TcpSessionService::TcpSessionService(nbio::net::TcpConnector connector)
    : connector_(std::move(connector)),
    receive_channel_(connector_, nbio::runtime::multiplexer(), nbio::runtime::scheduler()),
    send_channel_(connector_, nbio::runtime::multiplexer(), nbio::runtime::scheduler()),
      id_(next_id_.fetch_add(1, std::memory_order_acq_rel)) {}

TcpSessionService::~TcpSessionService() noexcept = default;

void TcpSessionService::Closee() noexcept {
    // The socket's own shutdown, then its close: a session is over once the descriptor
    // is gone, and the channels go on referring to a connection that is no longer
    // there -- which is what a reader still waiting on it needs to be told. Neither
    // reports anything worth acting on here.
    (void)connector_.Shutdown();
    connector_.Close();
}

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> TcpSessionService::Receive(
    std::span<char> buffer) {
    return receive_channel_.Receive(buffer);
}

nbio::async::Task<nbio::runtime, utility::expected<std::size_t, std::error_code>> TcpSessionService::Send(
    std::span<const char> buffer) {
    return send_channel_.Send(buffer);
}

std::atomic_uint TcpSessionService::next_id_{0};
}  // namespace nbio::net




