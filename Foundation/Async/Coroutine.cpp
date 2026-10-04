#include "Coroutine.hpp"

#include <atomic>

#include "Scheduler.hpp"

namespace Foundation::Async {
void CoroutineControlBlock::cancel() noexcept {
    int expected{kAlive};
    if (state.compare_exchange_strong(expected, kCancelled, std::memory_order_acq_rel)) {
        scheduler->reclaim(*this);
    }
}

void CoroutineControlBlock::finish() noexcept {
    int expected{kAlive};
    if (state.compare_exchange_strong(expected, kFinished, std::memory_order_acq_rel)) {
        scheduler->reclaim(*this);
    }
}

CoroutineControlBlock::~CoroutineControlBlock() noexcept {
    if (auto handle = std::exchange(root, {})) {
        handle.destroy();
    }
}

void CoroutineToken::cancel() {
    if (const std::shared_ptr<CoroutineControlBlock> ccb = coroutine_control_block_.lock()) {
        ccb->cancel();
    }
}

bool CoroutineToken::is_dead() const noexcept {
    const std::shared_ptr<CoroutineControlBlock> ccb = coroutine_control_block_.lock();
    return !ccb || ccb->is_dead();
}
}  // namespace Foundation::Async
