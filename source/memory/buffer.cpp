#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory/buffer.hpp>

namespace memory {
Buffer::Buffer(std::size_t capacity, std::size_t max_capacity)
    :  // begin_/end_ start at base_, so the storage must at least hold that.
      capacity_(std::min(capacity, max_capacity)),
      max_capacity_(max_capacity),
      storage_(std::make_unique<char[]>(capacity_)) {}

Buffer::~Buffer() noexcept = default;

bool Buffer::Reserve(std::size_t size) {
    if (writable_size() >= size) {
        return true;
    }
    const std::size_t held = readable_size();
    if (size > max_capacity_ - held) {
        return false;
    }
    if (size <= capacity_ - held) {
        if (held != 0) {
            std::memmove(storage_.get(), storage_.get() + begin_, held);
        }
        begin_ = 0;
        end_ = held;
        return true;
    }
    const std::size_t minimal = held + size;
    const std::size_t capacity =
        std::max(minimal, capacity_ + std::min(capacity_ / 2, max_capacity_ - capacity_));
    // move the existing data to the beginning of the new storage, leaving no space for prepending
    // since appending is more frequent. The region the buffer describes moves
    // with it: bytes that are read but not yet consumed sit at the front of the
    // new storage now, so begin_ and end_ follow them there. Leaving them where
    // they were would have the buffer read a part of the new storage its bytes
    // were never copied to.
    auto storage = std::make_unique<char[]>(capacity);
    if (held != 0) {
        std::memcpy(storage.get(), storage_.get() + begin_, held);
    }
    storage_ = std::move(storage);
    capacity_ = capacity;
    begin_ = 0;
    end_ = held;
    return true;
}

bool Buffer::Write(const char* data, std::size_t size) {
    if (size == 0) {
        return true;
    }
    if (!Reserve(size)) {
        return false;
    }
    std::memcpy(storage_.get() + end_, data, size);
    end_ += size;
    return true;
}

bool Buffer::Write(const char* data) { return Buffer::Write(data, std::strlen(data)); }

void Buffer::Consume(std::size_t size) noexcept {
    assert(size <= readable_size());
    begin_ += std::min(size, readable_size());
    if (is_empty()) {
        Clear();
    }
}

void Buffer::Shrink() {
    const std::size_t held = readable_size();
    // Minimum capacity that still honours the reserved prefix.
    const std::size_t capacity = held;
    if (capacity >= capacity_) {
        return;  // shrinking must never grow
    }
    auto storage = std::make_unique<char[]>(capacity);
    if (held != 0) {
        std::memcpy(storage.get(), storage_.get() + begin_, held);
    }
    storage_ = std::move(storage);
    capacity_ = capacity;
    begin_ = 0;
    end_ = held;
}
}  // namespace memory
