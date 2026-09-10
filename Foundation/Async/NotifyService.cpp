#include "NotifyService.hpp"
#include <Foundation/Async/NotifyChannel.hpp>

namespace Foundation::Async
{
NotifyService::NotifyService(Multiplexer& multiplexer, Scheduler& scheduler) :
    channel_(notifier_, multiplexer, scheduler)
{
}

NotifyService::~NotifyService() noexcept = default;
} // namespace Foundation::Async