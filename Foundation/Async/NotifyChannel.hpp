#pragma once

#include <Foundation/Async/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Notifier.hpp>

#include "Channel.hpp"
#include <coroutine>
#include <cstdint>
#include <mutex>
#include <vector>

namespace Foundation::Async
{
class NotifyChannel;

class NotifyChannel final : public Channel
{
public:
    NotifyChannel(Notifier& notifier, Multiplexer& multiplexer, Scheduler& scheduler);
    ~NotifyChannel() noexcept;

    template <typename It>
    void submit(It begin, It end);

    virtual void on_event() override;
    void submit(std::coroutine_handle<> h);

    Notifier& notifier() noexcept
    {
        return notifier_;
    }
    const Notifier& notifier() const noexcept
    {
        return notifier_;
    }

    std::uint64_t &count() noexcept
    {
        return count_;
    }
    const std::uint64_t &count() const noexcept
    {
        return count_;
    }

private:
    Notifier& notifier_;
    std::mutex mutex_;
    std::vector<std::coroutine_handle<>> raw_notifiees_;
    std::uint64_t count_{};
};

template <typename It>
void NotifyChannel::submit(It begin, It end)
{
    std::lock_guard lock(mutex_);
    raw_notifiees_.insert(raw_notifiees_.end(), begin, end);
    notifier_.notify();
}
} // namespace Foundation::Async