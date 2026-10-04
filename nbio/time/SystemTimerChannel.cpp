#include "SystemTimerChannel.hpp"

namespace nbio::time {
SystemTimerChannel::SystemTimerChannel(nbio::time::SystemTimer& timer, nbio::core::Multiplexer& multiplexer,
                                       nbio::async::Scheduler& scheduler)
    : nbio::core::Channel<SystemTimerChannel>(nbio::core::ChannelType::kSystemTimer, timer.native_handle(),
                                                    multiplexer, scheduler),
      timer_(timer),
      queue_() {
    timer_.NonBlocking(true);
    // Registered on the first arm(): nothing to watch until a sleep queues.
}

SystemTimerChannel::~SystemTimerChannel() noexcept { multiplexer_.DeleteChannel(this); }

void SystemTimerChannel::Park(async::Coroutine coroutine, std::chrono::steady_clock::time_point due) {
    if (due <= std::chrono::steady_clock::now()) {
        const bool was_empty = immediate_queue_.empty();
        immediate_queue_.push(std::move(coroutine));
        if (was_empty) {
            timer_.fire_after(std::chrono::nanoseconds{1});
            Arm();
        }
        return;
    }

    queue_.emplace(coroutine, due);
    timer_.fire_at(queue_.top().timepoint);
}

SystemTimerChannel::Payload& SystemTimerChannel::Submit() { return payload_; }

bool SystemTimerChannel::remove(async::Coroutine coroutine_view) {
    bool removed_immediate = false;
    std::queue<async::Coroutine> filtered_immediate;
    while (!immediate_queue_.empty()) {
        auto current = std::move(immediate_queue_.front());
        immediate_queue_.pop();
        if (current.handle == coroutine_view.handle) {
            removed_immediate = true;
            continue;
        }
        filtered_immediate.push(std::move(current));
    }
    immediate_queue_ = std::move(filtered_immediate);

    const bool removed_delayed = queue_.remove_if(
        [&](const detail::SystemTimerEntry& entry) { return entry.coroutine_view.handle == coroutine_view.handle; });
    const bool removed = removed_immediate || removed_delayed;
    if (removed) {
        if (!immediate_queue_.empty()) {
            if (!is_armed()) {
                Arm();
            }
            timer_.fire_after(std::chrono::nanoseconds{1});
        } else if (queue_.is_empty()) {
            timer_.cancel();
            if (is_armed()) {
                Disarm();
            }
        } else {
            timer_.fire_at(queue_.top().timepoint);
        }
    }
    return removed;
}

void SystemTimerChannel::Complete() {
    auto& payload = payload_;
    payload.release_poll();

    // Take the expirations out first: that is what makes the timerfd stop
    // reporting, and the entries below are the ones it is reporting for.
    auto result = timer_.wait();
    if (!result) {
        throw std::runtime_error(std::format("failed to wait timer: {}", result.error().message()));
    }

    while (!queue_.is_empty()) [[likely]] {
        const auto& top = queue_.top();
        if (top.timepoint > std::chrono::steady_clock::now()) {
            break;
        }
        auto& cv = top.coroutine_view;
        scheduler_.Submit(std::move(cv));
        queue_.pop();
    }

    while (!immediate_queue_.empty()) {
        scheduler_.Submit(std::move(immediate_queue_.front()));
        immediate_queue_.pop();
    }

    if (!queue_.is_empty()) {
        auto tp = queue_.top().timepoint;
        auto dur = tp - std::chrono::steady_clock::now();
        if (dur < std::chrono::milliseconds(0)) {
            dur = std::chrono::milliseconds(0);
        }
        timer_.fire_after(dur);
        Arm();
    } else {
        // Nothing left to wait for.
        Disarm();
    }
}
}  // namespace nbio::time




