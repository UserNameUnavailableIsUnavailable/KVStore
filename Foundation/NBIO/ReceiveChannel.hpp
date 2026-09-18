#pragma once

#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <deque>
#include <span>
#include <sys/uio.h>
#include <system_error>
#include <vector>

namespace Foundation::NBIO
{
// One receive waiting its turn. The buffer and the slot its outcome is written
// into belong to the frame that is parked; several of these are filled by one
// readv, in the order they were queued.
struct PendingReceive
{
    std::span<char> buffer;
    Foundation::Core::ReceiveResult *result{nullptr};
    Foundation::Async::Coroutine waiter;
};

// Simplex channel dedicated to receiving: a queue of receives, one operation, with
// the channel interested only in the "readable" event.
class ReceiveChannel : public Foundation::NBIO::Channel
{
  public:
    explicit ReceiveChannel(Foundation::Core::Socket &socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler);
    ~ReceiveChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> receive(std::span<char> buffer);

    void handle_event() override;

    // Hands a receive to the channel: it joins the readv in flight when there is
    // one, and starts one otherwise.
    void submit(std::span<char> buffer, Foundation::Core::ReceiveResult &result, Async::Coroutine waiter);

    // The receives the kernel has been given, in the order they will be filled.
#if defined(__linux)

    const ::msghdr &message_batch() const noexcept
    {
        return message_;
    }

    ::msghdr &message_batch() noexcept
    {
        return message_;
    }
#endif
    // What arrived: a byte count, or a negative errno. Nothing at all means the
    // peer closed, which is the answer to every receive waiting behind it too.
    void complete(std::ptrdiff_t result) noexcept;

    // Does the armed receives here and now, for a multiplexer that has to drive
    // the socket itself rather than wait for a completion.
    void flush() noexcept;

    Foundation::Core::Socket &socket() noexcept
    {
        return socket_;
    }
    const Foundation::Core::Socket &socket() const noexcept
    {
        return socket_;
    }
    std::error_code last_error() const noexcept
    {
        return error_code_;
    }

  private:
    void arm_batch();
    void refresh_vectors();
    void fail(PendingReceive &pending) noexcept;

    Foundation::Core::Socket &socket_;
    std::deque<PendingReceive> pending_;
#if defined(__linux__)
    std::vector<::iovec> vectors_;
    ::msghdr message_{};
#endif
    std::size_t armed_{0};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
