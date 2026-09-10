#include "Buffer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>

namespace Foundation
{
Buffer::Buffer(std::size_t capacity, std::size_t max_capacity)
    : // begin_/end_ start at base_, so the storage must at least hold that.
      capacity_(std::min(capacity, max_capacity)), max_capacity_(max_capacity),
      base_(std::min(capacity_, kDefaultBase)), storage_(std::make_unique<char[]>(capacity_))
{
}

Buffer::~Buffer() noexcept = default;

bool Buffer::reserve_for_prepend(std::size_t size)
{
    if (prependable_size() >= size)
    {
        return true;
    }
    const std::size_t held = valid_size();
    const std::size_t extra = size - prependable_size();
    if (capacity_ - held >= size)
    {
        std::memmove(storage_.get() + begin_ + extra, storage_.get() + begin_, held);
        begin_ += extra;
        end_ += extra;
        return true;
    }
    // Neither end has room: grow, keeping the tail room and opening the gap.
    const std::size_t minimal = held + size;
    if (minimal > max_capacity_)
    {
        return false;
    }

    // bargaining if minimal requirement satisfied
    std::size_t capacity = minimal;
    if (capacity + capacity / 2 < max_capacity_)
    {
        capacity = capacity + capacity / 2;
    }
    auto storage = std::make_unique<char[]>(capacity);
    // satisfy the prepend requirement only, since appending is more frequent
    std::memcpy(storage.get() + begin_ + extra, storage_.get() + begin_, held);
    begin_ += extra;
    end_ += extra;
    storage_ = std::move(storage);
    capacity_ = capacity;
    return true;
}

bool Buffer::reserve_for_append(std::size_t size)
{
    if (appendable_size() >= size)
    {
        return true;
    }
    const std::size_t held = valid_size();
    const std::size_t extra = size - appendable_size();
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

bool Buffer::prepend(const char *data, std::size_t size)
{
    if (size == 0)
    {
        return true;
    }
    if (!reserve_for_prepend(size))
    {
        return false;
    }
    begin_ -= size;
    std::memcpy(storage_.get() + begin_, data, size);
    return true;
}

bool Buffer::prepend(const char *data)
{
    return Buffer::prepend(data, std::strlen(data));
}

bool Buffer::append(const char *data, std::size_t size)
{
    if (size == 0)
    {
        return true;
    }
    if (!reserve_for_append(size))
    {
        return false;
    }
    std::memcpy(storage_.get() + end_, data, size);
    end_ += size;
    return true;
}

bool Buffer::append(const char *data)
{
    return Buffer::append(data, std::strlen(data));
}

void Buffer::consume(std::size_t size) noexcept
{
    assert(size <= valid_size());
    begin_ += std::min(size, valid_size());
}

void Buffer::set_base(std::size_t base)
{
    if (base > capacity_)
    {
        throw std::out_of_range("Base exceeds buffer capacity");
    }
    base_ = base;
}

void Buffer::shrink()
{
    const std::size_t held = valid_size();
    // Minimum capacity that still honours the reserved prefix.
    const std::size_t capacity = base_ + held;
    if (capacity >= capacity_)
    {
        return; // shrinking must never grow
    }
    auto storage = std::make_unique<char[]>(capacity);
    if (held != 0)
    {
        std::memcpy(storage.get() + base_, storage_.get() + begin_, held);
    }
    storage_ = std::move(storage);
    capacity_ = capacity;
    begin_ = base_;
    end_ = base_ + held;
}
} // namespace Foundation
