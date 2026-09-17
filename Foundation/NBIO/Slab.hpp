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
template <typename T, std::size_t SlotsPerBlock = 512> class Slab
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

        friend bool operator==(const Handle &, const Handle &) noexcept = default;

        bool IsValid() const noexcept
        {
            return generation != kInvalidGeneration;
        }

        // Pack into the 64 bits that io_uring (user_data) and epoll (data.u64)
        // round-trip for us untouched.
        std::uint64_t encode() const noexcept
        {
            return (static_cast<std::uint64_t>(generation) << 32) | index;
        }

        static Handle decode(std::uint64_t value) noexcept
        {
            return {.index = static_cast<IndexType>(value & 0xffff'ffffu),
                    .generation = static_cast<GenerationType>(value >> 32)};
        }
    };

    Slab() = default;

    // Copying would duplicate every object while every outstanding handle kept
    // pointing at the original, so it is not offered.
    Slab(const Slab &) = delete;
    Slab &operator=(const Slab &) = delete;

    // Moving is safe: only the block pointers move, never the blocks, so every
    // outstanding handle and every reference into the slab survives.
    Slab(Slab &&other) noexcept
        : blocks_(std::move(other.blocks_)), free_head_(std::exchange(other.free_head_, kInvalidIndex)),
          size_(std::exchange(other.size_, 0))
    {
    }

    Slab &operator=(Slab &&other) noexcept
    {
        if (this != &other)
        {
            clear(); // destroy what we hold before taking over the other's blocks
            blocks_ = std::move(other.blocks_);
            free_head_ = std::exchange(other.free_head_, kInvalidIndex);
            size_ = std::exchange(other.size_, 0);
        }
        return *this;
    }

    ~Slab()
    {
        clear();
    }

    // -------- Lifetime --------

    // Construct an object in a free slot, growing the slab if none is free.
    // Returns its handle together with a reference for immediate use, so the
    // caller does not have to look up what it just created.
    template <typename... Args> std::pair<Handle, T &> Acquire(Args &&...args)
    {
        if (free_head_ == kInvalidIndex)
        {
            add_block();
        }

        const IndexType index = free_head_;
        Slot &_slot = at(index);
        // Read the successor before constructing: if the constructor throws, the
        // slot must stay on the free list and the slab must look untouched.
        const IndexType next_free = _slot.next_free;

        std::construct_at(_slot.pointer(), std::forward<Args>(args)...);

        free_head_ = next_free;
        _slot.next_free = kInvalidIndex;
        _slot.occupied = true;
        ++size_;

        return {Handle{.index = index, .generation = _slot.generation}, *_slot.pointer()};
    }

    // Destroy the object a handle refers to and return its slot to the free
    // list. Returns false when the handle no longer refers to a live object,
    // which is the expected answer for a late event rather than an error.
    bool release(Handle handle) noexcept
    {
        Slot *slot = resolve(handle);
        if (slot == nullptr)
        {
            return false;
        }

        std::destroy_at(slot->pointer());
        slot->occupied = false;
        // This is the step that invalidates every handle ever minted for the
        // object that just went away.
        adcance_generation(*slot);
        slot->next_free = free_head_;
        free_head_ = handle.index;
        --size_;
        return true;
    }

    // -------- Lookup --------

    // Returns nullptr for a handle whose object is gone: an unset handle, an
    // out-of-range index, a released slot, or a slot that has since been reused.
    T *find(Handle handle) noexcept
    {
        Slot *slot = resolve(handle);
        return slot != nullptr ? slot->pointer() : nullptr;
    }

    const T *find(Handle handle) const noexcept
    {
        const Slot *slot = resolve(handle);
        return slot != nullptr ? slot->pointer() : nullptr;
    }

    bool contains(Handle handle) const noexcept
    {
        return resolve(handle) != nullptr;
    }

    // -------- Bulk access --------

    // Visit every live object. Meant for shutdown sweeps and diagnostics, not
    // for hot paths: it walks free slots too. Releasing the visited object from
    // inside the callback is allowed; acquiring during a walk is not, since the
    // new object may land in an already-visited slot.
    template <typename Visitor> void for_each(Visitor &&visit)
    {
        for (std::size_t index = 0; index < capacity(); ++index)
        {
            Slot &slot = at(static_cast<IndexType>(index));
            if (!slot.occupied)
            {
                continue;
            }
            visit(Handle{.index = static_cast<IndexType>(index), .generation = slot.generation}, *slot.pointer());
        }
    }

    // Read-only walk over every live object.
    template <typename Visitor> void for_each(Visitor &&visit) const
    {
        for (std::size_t index = 0; index < capacity(); ++index)
        {
            const Slot &slot = at(static_cast<IndexType>(index));
            if (!slot.occupied)
            {
                continue;
            }
            visit(Handle{.index = static_cast<IndexType>(index), .generation = slot.generation}, *slot.pointer());
        }
    }

    // -------- Observers --------

    std::size_t size() const noexcept
    {
        return size_;
    }
    bool is_empty() const noexcept
    {
        return size_ == 0;
    }
    // Slots allocated so far; grows in SlotsPerBlock steps and never shrinks.
    std::size_t capacity() const noexcept
    {
        return blocks_.size() * SlotsPerBlock;
    }

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

        T *pointer() noexcept
        {
            return std::launder(reinterpret_cast<T *>(storage));
        }
        const T *pointer() const noexcept
        {
            return std::launder(reinterpret_cast<const T *>(storage));
        }
    };

    struct Block
    {
        std::array<Slot, SlotsPerBlock> slots;
    };

    Slot &at(IndexType index) noexcept
    {
        return blocks_[index / SlotsPerBlock]->slots[index % SlotsPerBlock];
    }

    const Slot &at(IndexType index) const noexcept
    {
        return blocks_[index / SlotsPerBlock]->slots[index % SlotsPerBlock];
    }

    Slot *resolve(Handle handle) noexcept
    {
        return const_cast<Slot *>(static_cast<const Slab *>(this)->resolve(handle));
    }

    const Slot *resolve(Handle handle) const noexcept
    {
        if (handle.generation == kInvalidGeneration || handle.index >= capacity())
        {
            return nullptr;
        }

        const Slot &slot = at(handle.index);
        if (!slot.occupied || slot.generation != handle.generation)
        {
            return nullptr;
        }
        return &slot;
    }

    static void adcance_generation(Slot &slot) noexcept
    {
        ++slot.generation;
        if (slot.generation == kInvalidGeneration) // wrapped around
        {
            slot.generation = 1;
        }
    }

    // Append a block and thread its slots onto the front of the free list, so a
    // run of Acquire() calls keeps handing out neighbouring slots.
    void add_block()
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

    void clear() noexcept
    {
        for (const auto &block : blocks_)
        {
            for (Slot &slot : block->slots)
            {
                if (slot.occupied)
                {
                    std::destroy_at(slot.pointer());
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
