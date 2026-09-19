#pragma once
#if defined(__linux__)

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>
#include <Foundation/Core/RDMA_Stream.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include "Channel.hpp"

#include <optional>
#include <span>
#include <utility>

namespace Foundation::NBIO
{
class RDMA_ReceiveChannel final : public Channel
{
  public:
    using Handle = int;

        struct ReceiveJob
        {
                std::optional<std::span<char>> chunk{};
        };

    ~RDMA_ReceiveChannel() noexcept;

    RDMA_ReceiveChannel(Foundation::Core::RDMA_Stream &stream, Multiplexer &multiplexer,
                        Foundation::Async::Scheduler &scheduler);

    Foundation::NBIO::Task<std::optional<std::span<char>>> receive();

    Foundation::NBIO::Task<std::optional<std::span<char>>> try_receive();
    void release(std::span<char> chunk);

    void handle_completion();

    void park(Foundation::Async::Coroutine waiter) noexcept
    {
        waiter_ = std::move(waiter);
    }

    Foundation::Core::RDMA_Stream &stream() noexcept
    {
        return stream_;
    }

    ReceiveJob &job() noexcept
    {
        return job_;
    }

    const ReceiveJob &job() const noexcept
    {
        return job_;
    }

    private:
    Foundation::Core::RDMA_Stream &stream_;
    ReceiveJob job_{};
    Foundation::Async::Coroutine waiter_{};
};
} // namespace Foundation::NBIO

#endif // defined(__linux__)
