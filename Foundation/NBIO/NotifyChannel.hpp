#pragma once

#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Channel.hpp>
#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>
#include <Foundation/Async/Task.hpp>

#include <Foundation/Core/Notifier.hpp>

#include <cstdint>
#include <mutex>
#include <vector>

namespace Foundation::NBIO
{
class NotifyChannel;

class NotifyChannel final : public Foundation::NBIO::Channel
{
public:
    NotifyChannel(Foundation::Core::Notifier& notifier, Foundation::NBIO::Multiplexer& multiplexer, Foundation::Async::Scheduler& scheduler);
    ~NotifyChannel() noexcept;

    virtual void handle_event() override;

    template <typename It>
    void park(It begin, It end);

    void park(Foundation::Async::Coroutine notifiee);

    Foundation::Core::Notifier& notifier() noexcept
    {
        return notifier_;
    }

    const Foundation::Core::Notifier& notifier() const noexcept
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
    Foundation::Core::Notifier& notifier_;
    std::mutex mutex_;
    std::vector<Foundation::Async::Coroutine> notifiees_;
    std::uint64_t count_{};
};

template <typename It>
inline void NotifyChannel::park(It begin, It end)
{
    std::lock_guard lock(mutex_);
    notifiees_.insert(notifiees_.end(), begin, end);
    notifier_.notify();
}
} // namespace Foundation::NBIO
