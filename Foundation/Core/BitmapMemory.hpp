#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <vector>

namespace Foundation::Core
{
// A fixed block of memory partitioned into equal chunks, lent out one at a time.
//
// A chunk is named by an index, and that handle is what gets encoded into a work
// request id: a completion comes back carrying only those 64 bits. The bitmap
// says which chunks are out, so a handle that is not currently out is rejected
// rather than acted on -- but a handle used again after its chunk was released
// and handed to someone else looks exactly like the new owner's. Releasing a
// chunk is therefore a promise that nothing still refers to it, so drain
// completions before returning one.
//
// The block is allocated aligned, so it can be handed to a device registration
// without pinning pages it does not own.
//
// Not thread safe: one pool belongs to one event loop.
class BitmapMemory
{
  public:
    static constexpr std::size_t kAlignment = 4096;
    // Reserved, so a default-constructed handle never names a chunk.
    static constexpr std::uint32_t kInvalidIndex = std::numeric_limits<std::uint32_t>::max();

    struct Chunk
    {
        std::uint32_t index{kInvalidIndex};

        bool is_valid() const noexcept
        {
            return index != kInvalidIndex;
        }

        // What goes into a work request id, and comes back in the completion.
        std::uint64_t encode() const noexcept
        {
            return index;
        }

        static Chunk decode(std::uint64_t value) noexcept
        {
            return {.index = static_cast<std::uint32_t>(value)};
        }

        friend bool operator==(const Chunk &, const Chunk &) noexcept = default;
    };

    // `chunk_size` bytes per chunk, `chunk_count` chunks, one block.
    BitmapMemory(std::size_t chunk_size, std::size_t chunk_count);
    ~BitmapMemory() noexcept;

    BitmapMemory(const BitmapMemory &) = delete;
    BitmapMemory &operator=(const BitmapMemory &) = delete;
    BitmapMemory(BitmapMemory &&other) noexcept;
    BitmapMemory &operator=(BitmapMemory &&other) noexcept;

    // Hands out a free chunk, or nothing when every chunk is out.
    std::optional<Chunk> acquire() noexcept;

    // Returns a chunk. False for a handle that is not currently out: never
    // acquired, already released, or out of range.
    bool release(Chunk chunk) noexcept;

    // The chunk's bytes, or nothing for a handle that is not currently out.
    std::span<char> data(Chunk chunk) noexcept;
    std::span<const char> data(Chunk chunk) const noexcept;

    std::size_t chunk_size() const noexcept
    {
        return chunk_size_;
    }

    std::size_t capacity() const noexcept
    {
        return chunk_count_;
    }

    // The remaining credits.
    std::size_t available() const noexcept
    {
        return available_;
    }

    // The whole block, for handing to a device registration. Its size is
    // chunk_size() * capacity().
    char *storage() noexcept
    {
        return storage_.get();
    }

    const char *storage() const noexcept
    {
        return storage_.get();
    }

  private:
    struct AlignedDeleter
    {
        void operator()(char *pointer) const noexcept
        {
            ::operator delete(pointer, std::align_val_t{kAlignment});
        }
    };

    bool in_use(std::size_t index) const noexcept;
    void set_in_use(std::size_t index) noexcept;
    void clear_in_use(std::size_t index) noexcept;

    std::unique_ptr<char, AlignedDeleter> storage_{nullptr};
    std::size_t chunk_size_{0};
    std::size_t chunk_count_{0};
    std::size_t available_{0};
    // Where the next search starts, so a nearly full pool is the only case that
    // scans from the beginning.
    std::size_t search_hint_{0};
    std::vector<std::uint64_t> in_use_; // one bit per chunk
};
} // namespace Foundation::Core