#pragma once

#include <Foundation/Async/NotifyChannel.hpp>
#include <Foundation/Async/Multiplexer.hpp>
#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::Async
{
class NotifyService
{
public:
    NotifyService(Multiplexer& multiplexer, Scheduler& scheduler);
    ~NotifyService() noexcept;
    NotifyChannel &channel() noexcept
    {
        return channel_;
    }
    const NotifyChannel &channel() const noexcept
    {
        return channel_;
    }
private:
    Notifier notifier_;
    NotifyChannel channel_;
};
} // Async