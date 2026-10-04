#pragma once

#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/TcpSocket.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/EpollMultiplexer.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <chrono>
#include <filesystem>
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
// machinery lives in Foundation::Async.
namespace Foundation::NBIO {
inline bool is_initialized() { return Engine::is_initialized(); }

void initialize(std::unique_ptr<Multiplexer> multiplexer);

void run(Task<void> main);

template <typename T>
Foundation::Async::CoroutineToken spawn(Task<T> task) {
    return Foundation::Async::spawn(std::move(task));
}
}  // namespace Foundation::NBIO
