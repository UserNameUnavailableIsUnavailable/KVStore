#include "Scheduler.hpp"

#include <atomic>
#include <cassert>
#include <memory>
#include <utility>

namespace Foundation::Async {
void Scheduler::run() {
    bool expected{false};
    assert(running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel));

    idle_(ready_.empty() && reclaimed_.empty());
    std::swap(ready_, ready_batch_);

    for (const Coroutine& coroutine : ready_batch_) {
        const std::shared_ptr<CoroutineControlBlock> block = coroutine.lock();
        if (!block || block->is_dead() || !coroutine.handle) {
            continue;
        }
        coroutine.handle.resume();
    }
    ready_batch_.clear();

    std::list<std::shared_ptr<CoroutineControlBlock>> trash;
    std::swap(reclaimed_, trash);
    for (const std::shared_ptr<CoroutineControlBlock>& block : trash) {
        if (block->join && block->join_status != nullptr) {
            *block->join_status = block->is_cancelled() ? JoinStatus::kCancelled : JoinStatus::kCompleted;
            submit(std::move(block->join));
        }
    }
    trash.clear();

    running_.store(false, std::memory_order_release);
}

void Scheduler::reclaim(const CoroutineControlBlock& ccb) noexcept {
    assert(ccb.scheduler == this);
    reclaimed_.splice(reclaimed_.end(), roots_, ccb.index);
}
}  // namespace Foundation::Async
