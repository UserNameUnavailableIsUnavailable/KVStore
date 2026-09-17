#include "Buffer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>

namespace Foundation::Core
{
Buffer::Buffer(std::size_t capacity, std::size_t max_capacity)
    : // begin_/end_ start at base_, so the storage must at least hold that.
      capacity_(std::min(capacity, max_capacity)), max_capacity_(max_capacity),
      storage_(std::make_unique<char[]>(capacity_))
{
}

Buffer::~Buffer() noexcept = default;

bool Buffer::reserve(std::size_t size)
{
    if (writable_size() >= size)
    {
        return true;
    }
    const std::size_t held = readable_size();
    const std::size_t extra = size - writable_size();
    if (capacity_ - held >= size)
    {
        // Reclaim the whole consumed prefix: slide the live bytes all the way
        // to the front (the reserved prefix is fair game here -- base_ only
        // matters for Clear/Rebase). Compaction beats reallocation.
        std::memmove(storage_.get() + begin_ - extra, storage_.get() + begin_, held);
        begin_ = begin_ - extra;
        end_ = end_ - extra;
        return true;
    }
    const std::size_t minimal = held + size;
    if (minimal > max_capacity_)
    {
        return false;
    }
    // bargaining if the minimal requirement satisfied
    std::size_t capacity = minimal;
    if (capacity + capacity / 2 < max_capacity_)
    {
        capacity = capacity + capacity / 2;
    }
    auto storage = std::make_unique<char[]>(capacity);
    // move the existing data to the beginning of the new storage, leaving no space for prepending
    // since appending is more frequent
    std::memcpy(storage.get(), storage_.get() + begin_, held);
    storage_ = std::move(storage);
    capacity_ = capacity;
    begin_ = 0;
    end_ = held;
    return true;
}

bool Buffer::write(const char *data, std::size_t size)
{
    if (size == 0)
    {
        return true;
    }
    if (!reserve(size))
    {
        return false;
    }
    std::memcpy(storage_.get() + end_, data, size);
    end_ += size;
    return true;
}

bool Buffer::write(const char *data)
{
    return Buffer::write(data, std::strlen(data));
}

void Buffer::consume(std::size_t size) noexcept
{
    assert(size <= readable_size());
    begin_ += std::min(size, readable_size());
}

void Buffer::shrink()
{
    const std::size_t held = readable_size();
    // Minimum capacity that still honours the reserved prefix.
    const std::size_t capacity = held;
    if (capacity >= capacity_)
    {
        return; // shrinking must never grow
    }
    auto storage = std::make_unique<char[]>(capacity);
    if (held != 0)
    {
        std::memcpy(storage.get(), storage_.get() + begin_, held);
    }
    storage_ = std::move(storage);
    capacity_ = capacity;
    begin_ = 0;
    end_ = held;
}
} // namespace Foundation::Core
