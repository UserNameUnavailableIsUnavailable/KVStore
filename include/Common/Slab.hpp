#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace KV
{
// -------- Slab --------
// An object pool that names its objects with integer handles instead of
// pointers.
//
// It exists because an event-driven server has to name an object in places where
// a pointer cannot be trusted:
//   - io_uring carries a 64-bit user_data down into the kernel and back;
//   - epoll carries a 64-bit epoll_data;
//   - either one can surface long after the object it referred to is gone.
//
// A handle is an index plus a generation. The index locates the slot in O(1);
// the generation is bumped every time a slot is released, so a handle minted
// before that release can never resolve to whatever object took its place. A
// late completion therefore fails a Find() lookup instead of silently landing on
// a live object - which is precisely what a raw pointer, or a recycled file
// descriptor, cannot express.
//
// Objects never move. Slots live in fixed-size blocks and the slab only ever
// appends block pointers, so a reference handed out by Acquire() stays valid for
// as long as the object lives, across any number of later Acquire() calls. That
// is what lets a coroutine frame, an awaiter, or the kernel itself hold on to a
// buffer that lives inside a slab-allocated object.
//
// The slab manages object lifetime and nothing else. It has no idea what makes a
// particular object safe to release - draining pending I/O, waiting for a
// cancellation to be acknowledged, letting a coroutine finish - and it stays
// deliberately out of that decision. Such a policy belongs to the owner of the
// slab (a SessionManager, say), not here.
//
// Not thread safe: one slab belongs to one event loop.

template <typename T, std::size_t SlotsPerBlock = 512>
class Slab
{
public:
    using IndexType = std::uint32_t;
    using GenerationType = std::uint32_t;

    static constexpr GenerationType kInvalidGeneration = 0;
    static constexpr IndexType kInvalidIndex = std::numeric_limits<IndexType>::max();

    // Scoped to the slab's element type, so handles from different pools cannot
    // be confused for one another.
    struct Handle
    {
        IndexType index = 0;
        GenerationType generation = kInvalidGeneration;

        friend bool operator==(const Handle&, const Handle&) noexcept = default;

        bool IsValid() const noexcept { return generation != kInvalidGeneration; }

        // Pack into the 64 bits that io_uring (user_data) and epoll (data.u64)
        // round-trip for us untouched.
        std::uint64_t Encode() const noexcept
        {
            return (static_cast<std::uint64_t>(generation) << 32) | index;
        }

        static Handle Decode(std::uint64_t value) noexcept
        {
            return {
				.index = static_cast<IndexType>(value & 0xffff'ffffu),
                .generation = static_cast<GenerationType>(value >> 32)
			};
        }
    };

    Slab() = default;

    // Copying would duplicate every object while every outstanding handle kept
    // pointing at the original, so it is not offered.
    Slab(const Slab&) = delete;
    Slab& operator=(const Slab&) = delete;

    // Moving is safe: only the block pointers move, never the blocks, so every
    // outstanding handle and every reference into the slab survives.
    Slab(Slab&& other) noexcept :
        blocks_(std::move(other.blocks_)),
        free_head_(std::exchange(other.free_head_, kInvalidIndex)),
        size_(std::exchange(other.size_, 0))
    {
    }

    Slab& operator=(Slab&& other) noexcept
    {
        if (this != &other)
        {
            Clear(); // destroy what we hold before taking over the other's blocks
            blocks_ = std::move(other.blocks_);
            free_head_ = std::exchange(other.free_head_, kInvalidIndex);
            size_ = std::exchange(other.size_, 0);
        }
        return *this;
    }

    ~Slab() { Clear(); }

    // -------- Lifetime --------

    // Construct an object in a free slot, growing the slab if none is free.
    // Returns its handle together with a reference for immediate use, so the
    // caller does not have to look up what it just created.
    template <typename... Args>
    std::pair<Handle, T&> Acquire(Args&&... args)
    {
        if (free_head_ == kInvalidIndex)
        {
            AddBlock();
        }

        const IndexType index = free_head_;
        Slot& slot = GetSlot(index);
        // Read the successor before constructing: if the constructor throws, the
        // slot must stay on the free list and the slab must look untouched.
        const IndexType next_free = slot.next_free;

        std::construct_at(slot.Get(), std::forward<Args>(args)...);

        free_head_ = next_free;
        slot.next_free = kInvalidIndex;
        slot.occupied = true;
        ++size_;

        return {Handle {.index = index, .generation = slot.generation}, *slot.Get()};
    }

    // Destroy the object a handle refers to and return its slot to the free
    // list. Returns false when the handle no longer refers to a live object,
    // which is the expected answer for a late event rather than an error.
    bool Release(Handle handle) noexcept
    {
        Slot* slot = Resolve(handle);
        if (slot == nullptr)
        {
            return false;
        }

        std::destroy_at(slot->Get());
        slot->occupied = false;
        // This is the step that invalidates every handle ever minted for the
        // object that just went away.
        AdvanceGeneration(*slot);
        slot->next_free = free_head_;
        free_head_ = handle.index;
        --size_;
        return true;
    }

    // -------- Lookup --------

    // Returns nullptr for a handle whose object is gone: an unset handle, an
    // out-of-range index, a released slot, or a slot that has since been reused.
    T* Find(Handle handle) noexcept
    {
        Slot* slot = Resolve(handle);
        return slot != nullptr ? slot->Get() : nullptr;
    }

    const T* Find(Handle handle) const noexcept
    {
        const Slot* slot = Resolve(handle);
        return slot != nullptr ? slot->Get() : nullptr;
    }

    bool Contains(Handle handle) const noexcept { return Resolve(handle) != nullptr; }

    // -------- Bulk access --------

    // Visit every live object. Meant for shutdown sweeps and diagnostics, not
    // for hot paths: it walks free slots too. Releasing the visited object from
    // inside the callback is allowed; acquiring during a walk is not, since the
    // new object may land in an already-visited slot.
    template <typename Visitor>
    void ForEach(Visitor&& visit)
    {
        for (std::size_t index = 0; index < Capacity(); ++index)
        {
            Slot& slot = GetSlot(static_cast<IndexType>(index));
            if (!slot.occupied)
            {
                continue;
            }
            visit(Handle {.index = static_cast<IndexType>(index), .generation = slot.generation},
                *slot.Get());
        }
    }

    // Read-only walk over every live object.
    template <typename Visitor>
    void ForEach(Visitor&& visit) const
    {
        for (std::size_t index = 0; index < Capacity(); ++index)
        {
            const Slot& slot = GetSlot(static_cast<IndexType>(index));
            if (!slot.occupied)
            {
                continue;
            }
            visit(Handle {.index = static_cast<IndexType>(index), .generation = slot.generation},
                *slot.Get());
        }
    }

    // -------- Observers --------

    std::size_t Size() const noexcept { return size_; }
    bool Empty() const noexcept { return size_ == 0; }
    // Slots allocated so far; grows in SlotsPerBlock steps and never shrinks.
    std::size_t Capacity() const noexcept { return blocks_.size() * SlotsPerBlock; }

private:
    struct Slot
    {
        // A slot outlives the objects that pass through it, so the object is
        // constructed and destroyed explicitly rather than being a member.
        alignas(T) std::byte storage[sizeof(T)];
        // A slot's first handle is generation 1, since 0 means "no object".
        GenerationType generation = 1;
        IndexType next_free = kInvalidIndex; // only meaningful while free
        bool occupied = false;

        T* Get() noexcept { return std::launder(reinterpret_cast<T*>(storage)); }
        const T* Get() const noexcept { return std::launder(reinterpret_cast<const T*>(storage)); }
    };

    struct Block
    {
        std::array<Slot, SlotsPerBlock> slots;
    };

    Slot& GetSlot(IndexType index) noexcept
    {
        return blocks_[index / SlotsPerBlock]->slots[index % SlotsPerBlock];
    }

    const Slot& GetSlot(IndexType index) const noexcept
    {
        return blocks_[index / SlotsPerBlock]->slots[index % SlotsPerBlock];
    }

    Slot* Resolve(Handle handle) noexcept
    {
        return const_cast<Slot*>(static_cast<const Slab*>(this)->Resolve(handle));
    }

    const Slot* Resolve(Handle handle) const noexcept
    {
        if (handle.generation == kInvalidGeneration || handle.index >= Capacity())
        {
            return nullptr;
        }

        const Slot& slot = GetSlot(handle.index);
        if (!slot.occupied || slot.generation != handle.generation)
        {
            return nullptr;
        }
        return &slot;
    }

    static void AdvanceGeneration(Slot& slot) noexcept
    {
        ++slot.generation;
        if (slot.generation == kInvalidGeneration) // wrapped around
        {
            slot.generation = 1;
        }
    }

    // Append a block and thread its slots onto the front of the free list, so a
    // run of Acquire() calls keeps handing out neighbouring slots.
    void AddBlock()
    {
        const std::size_t base = blocks_.size() * SlotsPerBlock;
        if (base + SlotsPerBlock >= static_cast<std::size_t>(kInvalidIndex))
        {
            throw std::length_error("slab index space exhausted");
        }

        auto block = std::make_unique<Block>();
        for (std::size_t offset = 0; offset < SlotsPerBlock; ++offset)
        {
            const std::size_t index = base + offset;
            block->slots[offset].next_free = offset + 1 < SlotsPerBlock
                ? static_cast<IndexType>(index + 1)
                : free_head_; // last slot chains onto whatever was free before
        }

        // Grow the vector first: nothing may throw once free_head_ has moved.
        blocks_.reserve(blocks_.size() + 1);
        blocks_.push_back(std::move(block));
        free_head_ = static_cast<IndexType>(base);
    }

    void Clear() noexcept
    {
        for (const auto& block : blocks_)
        {
            for (Slot& slot : block->slots)
            {
                if (slot.occupied)
                {
                    std::destroy_at(slot.Get());
                    slot.occupied = false;
                }
            }
        }
        blocks_.clear();
        free_head_ = kInvalidIndex;
        size_ = 0;
    }

    std::vector<std::unique_ptr<Block>> blocks_;
    IndexType free_head_ = kInvalidIndex;
    std::size_t size_ = 0;
};
} // namespace KV
