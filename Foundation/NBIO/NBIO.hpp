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
#include "TcpAcceptChannel.hpp"
#include "RdmaSession.hpp"
#include "Runtime.hpp"
#include "TcpSession.hpp"

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

Task<void> sleep_until(std::chrono::steady_clock::time_point time_point);

Task<void> sleep_for(std::chrono::steady_clock::duration duration);

// Suspends until SIGINT/SIGTERM is delivered. The common shutdown idiom is:
// co_await when_any(AcceptLoop(), wait_for_signal()).
Task<void> wait_for_signal();

std::shared_ptr<FileStream> open_file(const std::filesystem::path &p);

std::unique_ptr<TcpAcceptChannel> bind(const Foundation::Core::SocketAddress &address, int backlog = 4096);

std::shared_ptr<TcpSession> establish(Foundation::Core::TcpSocket socket);
} // namespace Foundation::NBIO
