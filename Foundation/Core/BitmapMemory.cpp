#include "BitmapMemory.hpp"

#include <bit>
#include <limits>
#include <stdexcept>
#include <utility>

namespace Foundation::Core
{
namespace
{
constexpr std::size_t kBitsPerWord = 64;

std::size_t round_up(std::size_t value, std::size_t alignment) noexcept
{
    return (value + alignment - 1) & ~(alignment - 1);
}
} // namespace

BitmapMemory::BitmapMemory(std::size_t chunk_size, std::size_t chunk_count)
{
    if (chunk_size == 0 || chunk_count == 0)
    {
        throw std::invalid_argument("BitmapMemory needs a non-zero chunk size and count");
    }
    if (chunk_count > std::numeric_limits<std::uint32_t>::max())
    {
        throw std::length_error("BitmapMemory chunk count does not fit a handle");
    }
    if (chunk_count > std::numeric_limits<std::size_t>::max() / chunk_size)
    {
        throw std::length_error("BitmapMemory block size overflows");
    }

    const std::size_t bytes = round_up(chunk_size * chunk_count, kAlignment);
    storage_.reset(static_cast<char *>(::operator new(bytes, std::align_val_t{kAlignment})));

    in_use_.assign((chunk_count + kBitsPerWord - 1) / kBitsPerWord, 0);

    chunk_size_ = chunk_size;
    chunk_count_ = chunk_count;
    available_ = chunk_count;
}

BitmapMemory::~BitmapMemory() noexcept = default;

BitmapMemory::BitmapMemory(BitmapMemory &&other) noexcept
    : storage_(std::move(other.storage_)), chunk_size_(std::exchange(other.chunk_size_, 0)),
      chunk_count_(std::exchange(other.chunk_count_, 0)), available_(std::exchange(other.available_, 0)),
      search_hint_(std::exchange(other.search_hint_, 0)), in_use_(std::move(other.in_use_))
{
}

BitmapMemory &BitmapMemory::operator=(BitmapMemory &&other) noexcept
{
    if (this != &other)
    {
        storage_ = std::move(other.storage_);
        chunk_size_ = std::exchange(other.chunk_size_, 0);
        chunk_count_ = std::exchange(other.chunk_count_, 0);
        available_ = std::exchange(other.available_, 0);
        search_hint_ = std::exchange(other.search_hint_, 0);
        in_use_ = std::move(other.in_use_);
    }
    return *this;
}

std::optional<BitmapMemory::Chunk> BitmapMemory::acquire() noexcept
{
    if (available_ == 0)
    {
        return std::nullopt;
    }

    const std::size_t words = in_use_.size();
    for (std::size_t step = 0; step < words; ++step)
    {
        const std::size_t word = (search_hint_ + step) % words;
        std::uint64_t free_bits = ~in_use_[word];

        if (word == words - 1)
        {
            // Bits past the last chunk are not chunks. Masking them out keeps an
            // index beyond the pool from ever being handed out.
            const std::size_t used = chunk_count_ % kBitsPerWord;
            if (used != 0)
            {
                free_bits &= (std::uint64_t{1} << used) - 1;
            }
        }
        if (free_bits == 0)
        {
            continue;
        }

        const std::size_t index = word * kBitsPerWord + static_cast<std::size_t>(std::countr_zero(free_bits));
        set_in_use(index);
        --available_;
        search_hint_ = word;
        return Chunk{.index = static_cast<std::uint32_t>(index)};
    }

    return std::nullopt;
}

bool BitmapMemory::release(Chunk chunk) noexcept
{
    if (chunk.index >= chunk_count_ || !in_use(chunk.index))
    {
        return false; // never acquired, already released, or out of range
    }

    clear_in_use(chunk.index);
    ++available_;
    if (chunk.index < search_hint_)
    {
        search_hint_ = chunk.index;
    }
    return true;
}

std::span<char> BitmapMemory::data(Chunk chunk) noexcept
{
    if (chunk.index >= chunk_count_ || !in_use(chunk.index))
    {
        return {};
    }
    return {storage_.get() + static_cast<std::size_t>(chunk.index) * chunk_size_, chunk_size_};
}

std::span<const char> BitmapMemory::data(Chunk chunk) const noexcept
{
    if (chunk.index >= chunk_count_ || !in_use(chunk.index))
    {
        return {};
    }
    return {storage_.get() + static_cast<std::size_t>(chunk.index) * chunk_size_, chunk_size_};
}

bool BitmapMemory::in_use(std::size_t index) const noexcept
{
    return (in_use_[index / kBitsPerWord] >> (index % kBitsPerWord)) & 1u;
}

void BitmapMemory::set_in_use(std::size_t index) noexcept
{
    in_use_[index / kBitsPerWord] |= std::uint64_t{1} << (index % kBitsPerWord);
}

void BitmapMemory::clear_in_use(std::size_t index) noexcept
{
    in_use_[index / kBitsPerWord] &= ~(std::uint64_t{1} << (index % kBitsPerWord));
}
} // namespace Foundation::Core