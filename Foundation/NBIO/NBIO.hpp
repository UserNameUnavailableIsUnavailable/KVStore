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
namespace Foundation::NBIO
{
inline bool is_initialized()
{
    return Engine::is_initialized();
}

void initialize(std::unique_ptr<Multiplexer> multiplexer);

void run(Task<void> main);

template <typename T>
Foundation::Async::CoroutineToken spawn(Task<T> task)
{
    return Foundation::Async::spawn(std::move(task));
}

// Waiting is not a free function: the timer and the signal handling belong to the
// runtime, and SystemTimeService::sleep and SystemSignalService::wait are how a task
// reaches them.

// Internal: what a file's channels are built on. A caller opens a file through
// FileStreamService, and this is what that service is made of.
std::shared_ptr<FileStream> open_file(const std::filesystem::path &p);

// A listener is a TcpAcceptService and a connection is a TcpConnectService: neither is
// a free function, because each is attached to the engine of the thread that made it,
// and because what a caller is handed back is a session rather than a channel.
} // namespace Foundation::NBIO
