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
#include <sys/socket.h>
#include <sys/uio.h>
#include <system_error>
#include <vector>

struct io_uring_sqe;

namespace Foundation::NBIO
{
// One send waiting its turn. The bytes and the slot its outcome is written into
// belong to the frame that is parked; several of these go to the kernel in one
// sendmsg, because a stream has to keep their order.
struct PendingSend
{
    std::span<const char> buffer;
    Foundation::Core::SendResult *result{nullptr};
    Foundation::Async::Coroutine waiter;
};

// Simplex channel dedicated to sending: a queue of sends, one operation, with the
// channel interested only in the "writable" event.
class SendChannel final : public Foundation::NBIO::Channel
{
  public:
    explicit SendChannel(Foundation::Core::Socket &socket, Foundation::Async::Scheduler &scheduler, Foundation::NBIO::Multiplexer &multiplexer);
    ~SendChannel() noexcept override;

    Foundation::NBIO::Task<std::optional<std::size_t>> send(std::span<const char> buffer);

    // Sends as much of `buffer` as the socket will take right now, for a caller
    // that would rather not park: a send the kernel takes in one call never has
    // to reach the event loop. Answers the bytes written, or -1 when a batch is
    // already in flight and this send has to queue behind it rather than jump
    // ahead of the bytes already promised to the socket.
    std::ptrdiff_t send_now(std::span<const char> buffer) noexcept;

    void handle_event() override;

    // Hands a send to the channel: it joins the sendmsg in flight when there is
    // one, and starts one otherwise. The order is what a stream needs, so the
    // sends go in the order they were queued.
    void submit(std::span<const char> buffer, Foundation::Core::SendResult &result, Async::Coroutine waiter);

#if defined(__linux__)
    const ::msghdr &message_batch() const noexcept
    {
        return message_;
    }

    ::msghdr &message_batch() noexcept
    {
        return message_;
    }
#endif

    // What the kernel took: a byte count, or a negative errno.
    void complete(std::ptrdiff_t result) noexcept;

    // Does the armed sends here and now, for a multiplexer that has to drive the
    // socket itself rather than wait for a completion.
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
    void refresh_vectors();
    void arm_batch();
    void fail(PendingSend &pending) noexcept;

    Foundation::Core::Socket &socket_;
    std::deque<PendingSend> pending_;
#if defined(__linux__)
    std::vector<::iovec> vectors_;
    ::msghdr message_{};
#endif
    std::size_t armed_{0};
    std::error_code error_code_;
};
} // namespace Foundation::NBIO
