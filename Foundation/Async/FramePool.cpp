#include "FramePool.hpp"

#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <new>

namespace Foundation::Async
{
namespace
{
// Frames are the size of a promise plus the await chain's locals, which in this
// server are almost always well under 64 KiB. Anything larger goes straight to
// the global allocator; the rest round up to the next power of two.
constexpr std::size_t kMinClass = 64;
constexpr std::size_t kMinClassShift = 6;
constexpr std::size_t kMaxClass = 1U << 16U; // 64 KiB
constexpr std::size_t kNumClasses = kMinClassShift + 5; // 64, 128, ..., 65536 (11 classes)

// The pool keeps what it predicts the next epoch will ask for, plus a quarter.
//
// The prediction is an exponential average of how much a class is used, sampled
// once per epoch. With P the prediction and U the most the class had in use
// during the epoch that just ended,
//
//     P <- (P + U) / 2
//
// which forgets demand that has gone away at half the gap per epoch -- but it
// never predicts less than what was just seen, because a prediction below the
// demand that just happened has to be caught up with before anything can be
// served from the cache:
//
//     P <- max(U, (P + U) / 2)
//
// The headroom is what turns a prediction into a target: a quarter more than
// predicted is kept, so a burst does not have to wait for the average to move
// before the cache can serve it.
constexpr std::size_t kPredictionDenominator = 2;
constexpr std::size_t kHeadroomNumerator = 5; // 5/4
constexpr std::size_t kHeadroomDenominator = 4;
// Never fewer than one chunk, because a class that has been used once will be
// used again.
constexpr std::size_t kMinRetainedChunks = 1;

// The bound on the whole pool is what makes this a cache rather than a leak: a
// class whose frames stopped coming would otherwise keep them for the life of
// the process, and the 64 KiB class alone is megabytes at a hundred chunks.
constexpr std::size_t kRetainedBytesBudget = 4U * 1024U * 1024U;

// An epoch boundary could be found with a clock reading per release, but that
// costs more than the pool saves: one release in kEpochChecks looks at the
// clock, and an epoch ends at most once per kFramePoolEpoch.
constexpr std::size_t kEpochChecks = 256; // a power of two, so the check is a mask

struct FreeNode
{
    FreeNode *next;
};

class FramePool
{
  public:
    FramePool() = default;
    ~FramePool();

    FramePool(const FramePool &) = delete;
    FramePool &operator=(const FramePool &) = delete;

    void *allocate(std::size_t size)
    {
        const int index = class_of(size);
        if (index < 0)
        {
            return ::operator new(size);
        }
        Class &klass = classes_[static_cast<std::size_t>(index)];
        ++klass.used;
        if (klass.used > klass.epoch_peak)
        {
            klass.epoch_peak = klass.used; // what this epoch will be judged on
        }

        // A chunk that is already here is the whole point of the pool: this
        // request costs a pointer read instead of a trip to the allocator.
        if (klass.free_list == nullptr)
        {
            return ::operator new(class_size(index));
        }
        FreeNode *node = klass.free_list;
        klass.free_list = node->next;
        --klass.free;
        retained_bytes_ -= class_size(index);
        return node;
    }

    void deallocate(void *pointer, std::size_t size) noexcept
    {
        const int index = class_of(size);
        if (index < 0)
        {
            ::operator delete(pointer);
            return;
        }
        Class &klass = classes_[static_cast<std::size_t>(index)];
        if (klass.used > 0)
        {
            --klass.used;
        }
        epoch_if_due();

        // Over the target the pool is holding more than it is worth holding, so
        // this chunk goes back -- and one cached chunk goes back with it, which
        // drains a cache that demand has left behind at one chunk per release
        // instead of in one long burst on this frame's path.
        const std::size_t chunk = class_size(index);
        const std::size_t target = target_chunks(klass);
        if (klass.free < target && retained_bytes_ + chunk <= kRetainedBytesBudget)
        {
            push(klass, pointer, chunk);
            return;
        }
        ::operator delete(pointer);
        if (klass.free > target)
        {
            release_one(klass, chunk);
        }
    }

    FramePoolMetrics metrics() const noexcept
    {
        FramePoolMetrics result;
        for (const Class &klass : classes_)
        {
            result.live_frames += klass.used;
            result.cached_chunks += klass.free;
        }
        result.cached_bytes = retained_bytes_;
        return result;
    }

  private:
    // One size class: what it holds for reuse, how much of it is checked out, and
    // what the epochs so far say it will be asked for.
    struct Class
    {
        FreeNode *free_list{nullptr};
        std::size_t free{0};
        std::size_t used{0};
        std::size_t epoch_peak{0}; // the most in use at once during this epoch
        std::size_t predicted{0};  // the exponential average of the epochs so far
    };

    static int class_of(std::size_t size) noexcept
    {
        if (size <= kMinClass)
        {
            return 0;
        }
        const std::size_t cls = std::bit_ceil(size);
        if (cls > kMaxClass)
        {
            return -1;
        }
        return static_cast<int>(std::countr_zero(cls) - kMinClassShift);
    }

    static constexpr std::size_t class_size(int index) noexcept
    {
        return kMinClass << static_cast<unsigned>(index);
    }

    // What the cache should hold: a quarter more than predicted, so a burst has
    // somewhere to land, and never fewer than one chunk.
    static std::size_t target_chunks(const Class &klass) noexcept
    {
        const std::size_t wanted =
            (klass.predicted * kHeadroomNumerator + (kHeadroomDenominator - 1)) / kHeadroomDenominator;
        return wanted < kMinRetainedChunks ? kMinRetainedChunks : wanted;
    }

    void push(Class &klass, void *pointer, std::size_t chunk) noexcept
    {
        auto *node = static_cast<FreeNode *>(pointer);
        node->next = klass.free_list;
        klass.free_list = node;
        ++klass.free;
        retained_bytes_ += chunk;
    }

    void release_one(Class &klass, std::size_t chunk) noexcept
    {
        // Only called with free above the target, so the list has a node.
        FreeNode *node = klass.free_list;
        klass.free_list = node->next;
        --klass.free;
        retained_bytes_ -= chunk;
        ::operator delete(node);
    }

    void epoch_if_due() noexcept
    {
        if ((++checks_ & (kEpochChecks - 1)) != 0)
        {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now - last_epoch_ < kFramePoolEpoch)
        {
            return;
        }
        last_epoch_ = now;
        for (Class &klass : classes_)
        {
            close_epoch(klass);
        }
    }

    // The epoch is over: the prediction moves halfway to what the class actually
    // used -- never below it, so the demand that just happened is served next
    // time -- and the new epoch starts measuring from where this one ended.
    static void close_epoch(Class &klass) noexcept
    {
        const std::size_t average = (klass.predicted + klass.epoch_peak) / kPredictionDenominator;
        klass.predicted = klass.epoch_peak > average ? klass.epoch_peak : average;
        klass.epoch_peak = klass.used;
    }

    std::array<Class, kNumClasses> classes_{};
    std::size_t retained_bytes_{0};
    std::size_t checks_{0};
    std::chrono::steady_clock::time_point last_epoch_{std::chrono::steady_clock::now()};
};

FramePool::~FramePool()
{
    for (Class &klass : classes_)
    {
        for (FreeNode *node = klass.free_list; node != nullptr;)
        {
            FreeNode *next = node->next;
            ::operator delete(node);
            node = next;
        }
    }
}

FramePool &pool() noexcept
{
    thread_local FramePool pool;
    return pool;
}
} // namespace

void *frame_allocate(std::size_t size)
{
    return pool().allocate(size);
}

void frame_deallocate(void *pointer, std::size_t size) noexcept
{
    pool().deallocate(pointer, size);
}

FramePoolMetrics frame_pool_metrics() noexcept
{
    return pool().metrics();
}
} // namespace Foundation::Async
