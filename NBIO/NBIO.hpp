#pragma once

#include <NBIO/Async/Async.hpp>
#include <NBIO/Async/Coroutine.hpp>
#include <NBIO/Net/SocketAddress.hpp>
#include <NBIO/Net/TcpSocket.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Core/EpollMultiplexer.hpp>
#include <NBIO/Core/Multiplexer.hpp>
#include <NBIO/Runtime/Runtime.hpp>
#include <NBIO/Core/URingMultiplexer.hpp>
#include <NBIO/RDMA/RdmaAcceptChannel.hpp>
#include <NBIO/RDMA/RdmaConnectChannel.hpp>
#include <NBIO/Net/TcpConnectChannel.hpp>
#include <memory>

#include <NBIO/FS/FileStream.hpp>
#include <NBIO/FS/FileStreamService.hpp>
#include <NBIO/RDMA/RdmaDeliverService.hpp>
#include <NBIO/RDMA/RdmaSessionService.hpp>
#include <NBIO/Signal/SystemSignalService.hpp>
#include <NBIO/Time/SystemTimeService.hpp>
#include <NBIO/Net/TcpAcceptService.hpp>
#include <NBIO/Net/TcpConnectService.hpp>
#include <NBIO/Net/TcpSessionService.hpp>

// Umbrella header for the NBIO backend: the non-blocking I/O runtime built on
// epoll / io_uring. Everything here is backend-specific; the generic coroutine
// machinery lives in NBIO::Async.
namespace NBIO {
template <typename T>
using Task = NBIO::Async::Task<NBIO::Runtime, T>;

inline bool is_initialized() { return NBIO::Runtime::is_initialized(); }

inline void initialize(std::unique_ptr<Core::Multiplexer> multiplexer) {
    NBIO::Runtime::initialize(std::move(multiplexer));
}

inline void run(Task<void> main) { NBIO::Async::run(std::move(main)); }

template <typename T>
NBIO::Async::CoroutineToken spawn(Task<T> task) {
    return NBIO::Async::spawn(std::move(task));
}
}  // namespace NBIO
