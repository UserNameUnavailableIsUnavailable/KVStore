#pragma once
#if defined(__linux__)

#include <Foundation/Core/RDMA_Stream.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "RDMA_ReceiveChannel.hpp"
#include "RDMA_SendChannel.hpp"

#include <optional>
#include <span>

namespace Foundation::NBIO
{
class Multiplexer;

class RDMA_Session final
{
  public:
    RDMA_Session(Foundation::Core::RDMA_Stream stream, Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);

    RDMA_Session(const RDMA_Session &) = delete;
    RDMA_Session &operator=(const RDMA_Session &) = delete;
    RDMA_Session(RDMA_Session &&) = delete;
    RDMA_Session &operator=(RDMA_Session &&) = delete;
    ~RDMA_Session() noexcept = default;

    // Hands one acquired send chunk to the device and returns; poll_send() is
    // what waits for the completions that hand the chunks back.
    std::error_code send(std::span<char> chunk, std::size_t length);
    Task<std::size_t> poll_send(std::size_t count = 0);

    Task<std::optional<std::span<char>>> receive();

    // A message that has already arrived, without waiting for one: nothing when
    // none is ready. For a coroutine that has something else to do meanwhile.
    Task<std::optional<std::span<char>>> try_receive();

    void release(std::span<char> chunk);

    Foundation::Core::RDMA_Stream &stream() noexcept;
    const Foundation::Core::RDMA_Stream &stream() const noexcept;

    RDMA_SendChannel &send_channel() noexcept;
    const RDMA_SendChannel &send_channel() const noexcept;

    RDMA_ReceiveChannel &receive_channel() noexcept;
    const RDMA_ReceiveChannel &receive_channel() const noexcept;

  private:
    Foundation::Core::RDMA_Stream stream_;
    RDMA_SendChannel send_channel_;
    RDMA_ReceiveChannel receive_channel_;
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)