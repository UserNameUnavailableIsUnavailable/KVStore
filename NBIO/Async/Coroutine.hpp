#pragma once

#include <atomic>
#include <cassert>
#include <coroutine>
#include <list>
#include <memory>

namespace NBIO::Async {
class Scheduler;

struct CoroutineControlBlock;

// How a tree of coroutines ended, as seen by whoever parked on its token.
enum class JoinStatus {
    kCompleted,  // the root reached its final suspend point
    kCancelled,  // the tree was cancelled first
};

struct Coroutine {
    std::coroutine_handle<> handle{};

    std::weak_ptr<CoroutineControlBlock> control_block{};

    explicit operator bool() const noexcept;
    bool operator==(const Coroutine& other) const noexcept { return other.handle == handle; }

    // The block while the coroutine is alive; nothing once the scheduler has taken it
    // out of its registry. What a holder finds when the coroutine it named is over.
    std::shared_ptr<CoroutineControlBlock> lock() const noexcept { return control_block.lock(); }

    // Only a frame a scheduler drives can be named this way: spawn() gives the root its
    // block, and Task::Awaiter hands that same block down to every descendant.
    //
    // Written as one dependent expression, and deliberately: the block is incomplete
    // here, so naming its type in a local would make this fail to compile, where a
    // dependent expression is only checked once the template is instantiated.
    template <typename Promise>
    static Coroutine from_handle(std::coroutine_handle<Promise> handle) noexcept {
        assert(handle.promise().control_block != nullptr && "a coroutine outside a spawn tree cannot be parked");
        return Coroutine{handle, handle.promise().control_block->weak_from_this()};
    }
};

// The state of one tree of coroutines: its root frame, how the tree ended, and where
// the scheduler's registry holds it. Every frame of the tree shares it -- Task::Awaiter
// hands it down the await chain -- which is what makes a cancel at the root visible to
// a frame parked deep inside the tree.
struct CoroutineControlBlock : std::enable_shared_from_this<CoroutineControlBlock> {
    enum State {
        kAlive,
        kFinished,
        kCancelled,
    };

    std::coroutine_handle<> root{};  // the root frame: destroying the block destroys the whole tree
    Scheduler* scheduler{nullptr};
    std::list<std::shared_ptr<CoroutineControlBlock>>::iterator index;  // where the registry holds this block
    std::atomic_int state{kAlive};
    Coroutine join{};           // the coroutine parked on `co_await token`, if any
    JoinStatus* join_status{};  // where that joiner reads the outcome from, inside its own frame

    bool is_cancelled() const noexcept { return state.load(std::memory_order_acquire) == kCancelled; }
    bool is_finished() const noexcept { return state.load(std::memory_order_acquire) == kFinished; }
    bool is_dead() const noexcept { return state.load(std::memory_order_acquire) != kAlive; }

    void cancel() noexcept;
    void finish() noexcept;

    ~CoroutineControlBlock() noexcept;
};

class CoroutineToken {
   public:
    CoroutineToken() noexcept = default;
    explicit CoroutineToken(const std::shared_ptr<CoroutineControlBlock>& control_block) noexcept
        : coroutine_control_block_(control_block) {}

    void cancel();
    bool is_dead() const noexcept;

    struct JoinAwaiter {
        std::weak_ptr<CoroutineControlBlock> block;
        JoinStatus outcome{JoinStatus::kCompleted};  // written by the scheduler before it wakes us

        // A joiner destroyed while still parked -- its own tree was cancelled -- has to
        // take its name off the block it parked on, or the scheduler would wake a frame
        // that is no longer there.
        ~JoinAwaiter() noexcept {
            const std::shared_ptr<CoroutineControlBlock> alive = block.lock();
            if (alive != nullptr && alive->join_status == &outcome) {
                alive->join = Coroutine{};
                alive->join_status = nullptr;
            }
        }

        bool await_ready() const noexcept {
            const std::shared_ptr<CoroutineControlBlock> alive = block.lock();
            // Gone, or finished. A *cancelled* block whose removal has not been drained
            // yet is deliberately not ready: parking on it means being told kCancelled,
            // where answering here could only guess kCompleted.
            return !alive || alive->is_finished();
        }
        template <typename Promise>
        bool await_suspend(std::coroutine_handle<Promise> caller) noexcept {
            const std::shared_ptr<CoroutineControlBlock> alive = block.lock();
            if (!alive) {
                return false;  // over between the two questions: do not park for it
            }
            alive->join = Coroutine::from_handle(caller);  // resumed by the scheduler when the tree is over
            alive->join_status = &outcome;
            return true;
        }
        JoinStatus await_resume() const noexcept { return outcome; }
    };

    JoinAwaiter operator co_await() const noexcept { return JoinAwaiter{coroutine_control_block_}; }

   private:
    std::weak_ptr<CoroutineControlBlock> coroutine_control_block_;
};

inline Coroutine::operator bool() const noexcept {
    // The block first, and through the weak reference: the frame is destroyed with the
    // block, so asking the handle anything before asking whether the coroutine is still
    // live would be reading a frame that may be gone. `handle` alone is free and rules
    // out the default-constructed case.
    if (!handle) {
        return false;
    }
    const std::shared_ptr<CoroutineControlBlock> block = control_block.lock();
    return block && !block->is_dead() && !handle.done();
}

}  // namespace NBIO::Async
