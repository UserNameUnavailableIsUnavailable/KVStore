#pragma once

#include <nbio/async/Async.hpp>
#include <nbio/async/Coroutine.hpp>
#include <nbio/net/Address.hpp>
#include <nbio/net/TcpSocket.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/core/EpollMultiplexer.hpp>
#include <nbio/core/Multiplexer.hpp>
#include <nbio/runtime/Runtime.hpp>
#include <nbio/core/URingMultiplexer.hpp>
#include <nbio/net/RdmaAcceptChannel.hpp>
#include <nbio/net/RdmaConnectChannel.hpp>
#include <nbio/net/TcpConnectChannel.hpp>
#include <memory>

#include <nbio/fs/FileStream.hpp>
#include <nbio/fs/FileStreamService.hpp>
#include <nbio/net/RdmaDeliverService.hpp>
#include <nbio/net/RdmaSessionService.hpp>
#include <nbio/signal/SystemSignalService.hpp>
#include <nbio/time/SystemTimeService.hpp>
#include <nbio/net/TcpAcceptService.hpp>
#include <nbio/net/TcpConnectService.hpp>
#include <nbio/net/TcpSessionService.hpp>

// Umbrella header for the nbio backend: the non-blocking I/O runtime built on
// epoll / io_uring. Everything here is backend-specific; the generic coroutine
// machinery lives in nbio::async.
namespace nbio {
template <typename T>
using Task = nbio::async::Task<nbio::runtime, T>;

inline bool is_initialized() { return nbio::runtime::is_initialized(); }

inline void initialize(std::unique_ptr<core::Multiplexer> multiplexer) {
    nbio::runtime::initialize(std::move(multiplexer));
}

inline void run(Task<void> main) { nbio::async::run(std::move(main)); }

template <typename T>
nbio::async::CoroutineToken Spawn(Task<T> task) {
    return nbio::async::Spawn(std::move(task));
}
}  // namespace nbio
