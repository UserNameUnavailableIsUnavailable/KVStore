#pragma once

// Replication: the master keeps a live connection to each slave and streams
// mutations to it as they happen.
//
// A slave's socket is written by exactly ONE coroutine (the feed writer),
// because Async::SendChannel has a single waiter -- several client coroutines
// forwarding to the same slave concurrently would clash. So mutations do NOT
// send directly: a mutating client only PUSHes the encoded command into each
// slave's feed queue (synchronous, no I/O) and wakes that slave's writer. The
// writer drains the queue and performs the actual Send.
//
// Ordering / consistency: a new slave is seeded with the full dataset (as SET
// commands) into its queue, and only then registered for forwarding -- all
// without an intervening co_await, so being single-threaded makes "snapshot
// then stream" atomic and gap-free.

#include <deque>
#include <memory>
#include <string>
#include <utility>

#include <Foundation/Async/Engine.hpp>
#include <Foundation/Async/Session.hpp>
#include <Foundation/Async/Task.hpp>
#include "::Foundation::Buffer.hpp"

namespace KV
{
class ReplicationFeed
{
  public:
    ReplicationFeed(std::shared_ptr<Async::Session> session, Async::Scheduler &scheduler)
        : session_(std::move(session)), scheduler_(scheduler)
    {
    }

    // Enqueue an already-encoded command and wake the writer if it is parked.
    void Push(std::string bytes)
    {
        queue_.push_back(std::move(bytes));
        if (waiter_)
        {
            auto w = waiter_;
            waiter_ = {};
            scheduler_.submit(w);
        }
    }

    bool Closed() const noexcept
    {
        return closed_;
    }

    // The single writer coroutine for this slave: drain the queue and send.
    // `on_close` is invoked (once) when the slave goes away so the registry can
    // drop this feed. Spawn this and hand it the session's ownership.
    template <typename OnClose> Async::Task<void> run(OnClose on_close)
    {
        while (true)
        {
            std::string bytes = co_await PopAwaiter{this};
            auto buffer = std::make_unique<::Foundation::Buffer>(bytes.empty() ? 1 : bytes.size());
            buffer->append(bytes.data(), bytes.size());
            auto [buf, result] = co_await session_->send(std::move(buffer));
            (void)buf;
            if (result.status != ::SendStatus::kDone)
            {
                closed_ = true;
                on_close(this);
                co_return;
            }
        }
    }

  private:
    // Parks the writer when the queue is empty; Push() resumes it.
    struct PopAwaiter
    {
        ReplicationFeed *feed;
        bool await_ready() const noexcept
        {
            return !feed->queue_.empty();
        }
        void await_suspend(std::coroutine_handle<> handle) noexcept
        {
            feed->waiter_ = handle;
        }
        std::string await_resume()
        {
            std::string front = std::move(feed->queue_.front());
            feed->queue_.pop_front();
            return front;
        }
    };

    std::shared_ptr<Async::Session> session_;
    Async::Scheduler &scheduler_;
    std::deque<std::string> queue_;
    std::coroutine_handle<> waiter_{};
    bool closed_ = false;
};
} // namespace KV
