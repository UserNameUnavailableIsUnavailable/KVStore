#include "Coroutine.hpp"

#include <atomic>

#include "Scheduler.hpp"

namespace nbio::async {
void CoroutineControlBlock::Cancel() noexcept {
    int expected{kAlive};
    if (state.compare_exchange_strong(expected, kCancelled, std::memory_order_acq_rel)) {
        scheduler->Reclaim(*this);
    }
}

void CoroutineControlBlock::Finish() noexcept {
    int expected{kAlive};
    if (state.compare_exchange_strong(expected, kFinished, std::memory_order_acq_rel)) {
        scheduler->Reclaim(*this);
    }
}

CoroutineControlBlock::~CoroutineControlBlock() noexcept {
    if (auto handle = std::exchange(root, {})) {
        handle.destroy();
    }
}

void CoroutineToken::Cancel() {
    if (const std::shared_ptr<CoroutineControlBlock> ccb = coroutine_control_block_.lock()) {
        ccb->Cancel();
    }
}

bool CoroutineToken::IsDead() const noexcept {
    const std::shared_ptr<CoroutineControlBlock> ccb = coroutine_control_block_.lock();
    return !ccb || ccb->IsDead();
}
}  // namespace nbio::async
