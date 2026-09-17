#include "RDMA_Session.hpp"

namespace Foundation::NBIO
{
RDMA_Session::RDMA_Session(Foundation::Core::RDMA_Stream stream, Multiplexer &multiplexer,
                           Foundation::Async::Scheduler &scheduler) :
    stream_(std::move(stream)), send_channel_(stream_, multiplexer, scheduler), receive_channel_(stream_, multiplexer, scheduler)
{
}

std::error_code RDMA_Session::send(std::span<char> chunk, std::size_t length)
{
    return send_channel_.send(chunk, length);
}

Task<std::size_t> RDMA_Session::poll_send(std::size_t count)
{
    co_return co_await send_channel_.poll(count);
}

Task<std::optional<std::span<char>>> RDMA_Session::receive()
{
    co_return co_await receive_channel_.receive();
}

Task<std::optional<std::span<char>>> RDMA_Session::try_receive()
{
    co_return co_await receive_channel_.try_receive();
}

void RDMA_Session::release(std::span<char> chunk)
{
    receive_channel_.release(chunk);
}

Foundation::Core::RDMA_Stream &RDMA_Session::stream() noexcept
{
    return stream_;
}

const Foundation::Core::RDMA_Stream &RDMA_Session::stream() const noexcept
{
    return stream_;
}

RDMA_SendChannel &RDMA_Session::send_channel() noexcept
{
    return send_channel_;
}

const RDMA_SendChannel &RDMA_Session::send_channel() const noexcept
{
    return send_channel_;
}

RDMA_ReceiveChannel &RDMA_Session::receive_channel() noexcept
{
    return receive_channel_;
}

const RDMA_ReceiveChannel &RDMA_Session::receive_channel() const noexcept
{
    return receive_channel_;
}
} // namespace Foundation::NBIO