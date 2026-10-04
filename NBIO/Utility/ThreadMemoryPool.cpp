#include "ThreadMemoryPool.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>

namespace NBIO::Utility {
namespace {
#if defined(CUSTOM_MEMORY_POOLING) && defined(__linux__)
extern "C" {
void* __real_malloc(std::size_t) noexcept;
void* __real_calloc(std::size_t, std::size_t) noexcept;
void* __real_realloc(void*, std::size_t) noexcept;
void __real_free(void*) noexcept;
void* __real_posix_memalign(std::size_t, std::size_t) noexcept;
}
#endif

constexpr std::size_t kMinClass = 32;
constexpr std::size_t kMaxClass = 1U << 16U;  // 64 KiB
constexpr std::size_t kNumClasses = 200;
constexpr std::size_t kRetainedBytesBudget = 8U * 1024U * 1024U;

constexpr auto make_class_sizes() noexcept {
    std::array<std::size_t, kNumClasses> sizes{};
    std::size_t index = 0;

    // Small allocations are dense to minimize internal fragmentation.
    for (std::size_t size = 32; size <= 1024; size += 32) {
        sizes[index++] = size;
    }

    // Mid-sized allocations are still fairly dense.
    for (std::size_t size = 1152; size <= 8192; size += 128) {
        sizes[index++] = size;
    }

    // Large allocations use coarser classes to cap metadata and lookup costs.
    for (std::size_t size = 8704; size <= 65536; size += 512) {
        sizes[index++] = size;
    }

    return sizes;
}

constexpr std::array<std::size_t, kNumClasses> kClassSizes = make_class_sizes();
static_assert(kClassSizes.front() == kMinClass);
static_assert(kClassSizes.back() == kMaxClass);

constexpr std::size_t class_chunk_size(int index) noexcept { return kClassSizes[static_cast<std::size_t>(index)]; }

struct alignas(std::max_align_t) AllocationHeader {
    std::uint16_t class_index_plus_one{0};
    std::uint16_t reserved{0};
};

static_assert(sizeof(AllocationHeader) % alignof(std::max_align_t) == 0);

struct FreeNode {
    FreeNode* next;
};

#if defined(CUSTOM_MEMORY_POOLING) && defined(__linux__)
void* raw_malloc(std::size_t size) noexcept { return __real_malloc(size); }

void raw_free(void* pointer) noexcept { __real_free(pointer); }

void* raw_realloc(void* pointer, std::size_t size) noexcept { return __real_realloc(pointer, size); }

void* raw_memalign(std::size_t alignment, std::size_t size) noexcept {
    void* pointer = nullptr;
    if (posix_memalign(&pointer, alignment, size) != 0) {
        return nullptr;
    }
    return pointer;
}
#else
void* raw_malloc(std::size_t size) noexcept { return std::malloc(size); }

void raw_free(void* pointer) noexcept { std::free(pointer); }

void* raw_realloc(void* pointer, std::size_t size) noexcept { return std::realloc(pointer, size); }

void* raw_memalign(std::size_t alignment, std::size_t size) noexcept {
    void* pointer = nullptr;
    if (posix_memalign(&pointer, alignment, size) != 0) {
        return nullptr;
    }
    return pointer;
}
#endif

class ThreadMemoryPool {
   public:
    void* allocate(std::size_t size) {
        const std::size_t total = size + sizeof(AllocationHeader);
        const int index = class_of(total);
        if (index < 0) {
            void* raw = raw_malloc(total);
            if (raw == nullptr) {
                throw std::bad_alloc();
            }
            auto* header = static_cast<AllocationHeader*>(raw);
            header->class_index_plus_one = 0;
            return header + 1;
        }

        const std::size_t chunk = class_chunk_size(index);
        Class& klass = classes_[static_cast<std::size_t>(index)];
        void* raw = nullptr;
        if (klass.free_list != nullptr) {
            FreeNode* node = klass.free_list;
            klass.free_list = node->next;
            --klass.cached;
            cached_bytes_ -= chunk;
            raw = node;
        } else {
            raw = raw_malloc(chunk);
            if (raw == nullptr) {
                throw std::bad_alloc();
            }
        }

        auto* header = static_cast<AllocationHeader*>(raw);
        header->class_index_plus_one = static_cast<std::uint16_t>(index + 1);
        return header + 1;
    }

    void deallocate(void* pointer) noexcept {
        if (pointer == nullptr) {
            return;
        }

        auto* header = static_cast<AllocationHeader*>(pointer) - 1;
        if (header->class_index_plus_one == 0) {
            raw_free(header);
            return;
        }

        const std::size_t index = static_cast<std::size_t>(header->class_index_plus_one - 1);
        const std::size_t chunk = class_chunk_size(static_cast<int>(index));
        if (cached_bytes_ + chunk > kRetainedBytesBudget) {
            raw_free(header);
            return;
        }

        Class& klass = classes_[index];
        auto* node = static_cast<FreeNode*>(static_cast<void*>(header));
        node->next = klass.free_list;
        klass.free_list = node;
        ++klass.cached;
        cached_bytes_ += chunk;
    }

    ThreadMemoryPoolMetrics metrics() const noexcept {
        ThreadMemoryPoolMetrics out;
        for (const Class& klass : classes_) {
            out.cached_chunks += klass.cached;
        }
        out.cached_bytes = cached_bytes_;
        return out;
    }

    ~ThreadMemoryPool() noexcept {
        for (std::size_t index = 0; index < classes_.size(); ++index) {
            Class& klass = classes_[index];
            for (FreeNode* node = klass.free_list; node != nullptr;) {
                FreeNode* next = node->next;
                raw_free(node);
                node = next;
            }
            klass.free_list = nullptr;
            klass.cached = 0;
        }
        cached_bytes_ = 0;
    }

   private:
    struct Class {
        FreeNode* free_list{nullptr};
        std::size_t cached{0};
    };

    static int class_of(std::size_t size) noexcept {
        const auto it = std::lower_bound(kClassSizes.begin(), kClassSizes.end(), size);
        if (it == kClassSizes.end()) {
            return -1;
        }
        return static_cast<int>(it - kClassSizes.begin());
    }

    std::array<Class, kNumClasses> classes_{};
    std::size_t cached_bytes_{0};
};

ThreadMemoryPool& pool() noexcept {
    thread_local ThreadMemoryPool pool;
    return pool;
}

void* malloc_aligned(std::size_t size, std::size_t alignment) {
    void* pointer = raw_memalign(alignment, size);
    if (pointer == nullptr) {
        throw std::bad_alloc();
    }
    return pointer;
}
}  // namespace

void* pool_allocate(std::size_t size) { return pool().allocate(size); }

void pool_deallocate(void* pointer) noexcept { pool().deallocate(pointer); }

void* allocate_aligned(std::size_t size, std::size_t alignment) { return malloc_aligned(size, alignment); }

void raw_release(void* pointer) noexcept { raw_free(pointer); }

void* pool_calloc(std::size_t count, std::size_t size) {
    if (count != 0 && size > (static_cast<std::size_t>(-1) / count)) {
        throw std::bad_alloc();
    }
    const std::size_t total = count * size;
    void* pointer = pool().allocate(total);
    std::memset(pointer, 0, total);
    return pointer;
}

void* pool_realloc(void* pointer, std::size_t size) {
    if (pointer == nullptr) {
        return pool().allocate(size);
    }
    if (size == 0) {
        pool().deallocate(pointer);
        return nullptr;
    }
    auto* header = static_cast<AllocationHeader*>(pointer) - 1;
    if (header->class_index_plus_one == 0) {
        void* replacement = raw_realloc(header, size + sizeof(AllocationHeader));
        if (replacement == nullptr) {
            return nullptr;
        }
        auto* new_header = static_cast<AllocationHeader*>(replacement);
        new_header->class_index_plus_one = 0;
        return new_header + 1;
    }

    const std::size_t old_size =
        class_chunk_size(static_cast<int>(header->class_index_plus_one - 1)) - sizeof(AllocationHeader);
    if (size <= old_size) {
        return pointer;
    }
    void* replacement = pool().allocate(size);
    std::memcpy(replacement, pointer, old_size);
    pool().deallocate(pointer);
    return replacement;
}

ThreadMemoryPoolMetrics thread_memory_pool_metrics() noexcept { return pool().metrics(); }
}  // namespace NBIO::Utility

#if defined(CUSTOM_MEMORY_POOLING) && defined(__linux__)
void* operator new(std::size_t size) {
    try {
        return NBIO::Core::pool_allocate(size);
    } catch (...) {
        throw std::bad_alloc();
    }
}

extern "C" void* __wrap_malloc(std::size_t size) {
    try {
        return NBIO::Core::pool_allocate(size);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void* __wrap_calloc(std::size_t count, std::size_t size) {
    try {
        return NBIO::Core::pool_calloc(count, size);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void* __wrap_realloc(void* pointer, std::size_t size) {
    try {
        return NBIO::Core::pool_realloc(pointer, size);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void __wrap_free(void* pointer) noexcept { NBIO::Core::pool_deallocate(pointer); }

void* operator new[](std::size_t size) {
    try {
        return NBIO::Core::pool_allocate(size);
    } catch (...) {
        throw std::bad_alloc();
    }
}

void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return NBIO::Core::pool_allocate(size);
    } catch (...) {
        return nullptr;
    }
}

void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return NBIO::Core::pool_allocate(size);
    } catch (...) {
        return nullptr;
    }
}

void operator delete(void* pointer) noexcept { NBIO::Core::pool_deallocate(pointer); }

void operator delete[](void* pointer) noexcept { NBIO::Core::pool_deallocate(pointer); }

void operator delete(void* pointer, std::size_t) noexcept { NBIO::Core::pool_deallocate(pointer); }

void operator delete[](void* pointer, std::size_t) noexcept { NBIO::Core::pool_deallocate(pointer); }

void operator delete(void* pointer, const std::nothrow_t&) noexcept { NBIO::Core::pool_deallocate(pointer); }

void operator delete[](void* pointer, const std::nothrow_t&) noexcept { NBIO::Core::pool_deallocate(pointer); }

void* operator new(std::size_t size, std::align_val_t alignment) {
    try {
        return NBIO::Core::allocate_aligned(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        throw std::bad_alloc();
    }
}

void* operator new[](std::size_t size, std::align_val_t alignment) {
    try {
        return NBIO::Core::allocate_aligned(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        throw std::bad_alloc();
    }
}

void operator delete(void* pointer, std::align_val_t) noexcept { NBIO::Core::raw_release(pointer); }

void operator delete[](void* pointer, std::align_val_t) noexcept { NBIO::Core::raw_release(pointer); }

void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept { NBIO::Core::raw_release(pointer); }

void operator delete[](void* pointer, std::size_t, std::align_val_t) noexcept {
    NBIO::Core::raw_release(pointer);
}

void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try {
        return NBIO::Core::allocate_aligned(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        return nullptr;
    }
}

void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    try {
        return NBIO::Core::allocate_aligned(size, static_cast<std::size_t>(alignment));
    } catch (...) {
        return nullptr;
    }
}

void operator delete(void* pointer, std::align_val_t, const std::nothrow_t&) noexcept {
    NBIO::Core::raw_release(pointer);
}

void operator delete[](void* pointer, std::align_val_t, const std::nothrow_t&) noexcept {
    NBIO::Core::raw_release(pointer);
}
#endif