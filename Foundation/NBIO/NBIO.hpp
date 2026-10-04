#pragma once

#include <NBIO/Async/Async.hpp>
#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Core/SocketAddress.hpp>
#include <NBIO/Core/TcpSocket.hpp>
#include <NBIO/NBIO/Engine.hpp>
#include <NBIO/NBIO/EpollMultiplexer.hpp>
#include <NBIO/NBIO/Multiplexer.hpp>
#include <NBIO/NBIO/Runtime.hpp>
#include <NBIO/NBIO/RdmaAcceptChannel.hpp>
#include <NBIO/NBIO/RdmaConnectChannel.hpp>
#include <NBIO/NBIO/TcpConnectChannel.hpp>
#include <memory>

#include "FileStream.hpp"
#include "FileStreamService.hpp"
#include "RdmaDeliverService.hpp"
#include "RdmaSessionService.hpp"
#include "Runtime.hpp"
#include "SystemSignalService.hpp"
#include "SystemTimeService.hpp"
#include "TcpAcceptService.hpp"
#include "TcpConnectService.hpp"
#include "TcpSessionService.hpp"

// Umbrella header for the NBIO backend: the non-blocking I/O runtime built on
// epoll / io_uring. Everything here is backend-specific; the generic coroutine
// machinery lives in NBIO::Async.
namespace NBIO::NBIO {
inline bool is_initialized() { return Engine::is_initialized(); }

void initialize(std::unique_ptr<Multiplexer> multiplexer);

void run(Task<void> main);

template <typename T>
NBIO::Async::CoroutineToken spawn(Task<T> task) {
    return NBIO::Async::spawn(std::move(task));
}
}  // namespace NBIO::NBIO

