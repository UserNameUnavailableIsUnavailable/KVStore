#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

//   storage_:  [ prependable | valid | appendable ]
//              ^             ^       ^            ^
//              0             begin_  end_         capacity_
namespace Foundation::Core
{
class Buffer
{
  public:
    explicit Buffer(std::size_t initial_capacity = 512, std::size_t max_capacity = 4096);
    Buffer(const Buffer &) = delete;
    Buffer &operator=(const Buffer &) = delete;
    Buffer(Buffer &&) noexcept = default;
    Buffer &operator=(Buffer &&) = default;
    ~Buffer() noexcept;

    std::size_t writable_size() const noexcept
    {
        return capacity_ - end_;
    }

    std::span<char> writable_span() noexcept
    {
        return {storage_.get() + end_, storage_.get() + capacity_};
    }

    bool is_empty() const noexcept
    {
        return begin_ == end_;
    }

    std::string_view string_view() const noexcept
    {
        return {storage_.get() + begin_, storage_.get() + end_};
    }

    std::span<char> readable_span() noexcept
    {
        return {storage_.get() + begin_, storage_.get() + end_};
    }

    std::span<const char> readable_span() const noexcept
    {
        return {storage_.get() + begin_, storage_.get() + end_};
    }

    bool reserve(std::size_t size);
    bool write(const char *data, std::size_t size);
    bool write(const char *data);

    void commit(std::size_t size) noexcept
    {
        assert(size <= writable_size());
        end_ += size;
    }

    void consume(std::size_t size) noexcept;
    void consume_all() noexcept
    {
        begin_ = end_ = 0;
    }

    std::size_t capacity() const noexcept
    {
        return capacity_;
    }

    void clear() noexcept
    {
        begin_ = end_ = 0;
    }

    // shrink the buffer to the minimum capacity that can hold the data
    void shrink();

    [[nodiscard]] std::size_t readable_size() const noexcept
    {
        return end_ - begin_;
    }

    std::span<char> span(std::size_t begin, std::size_t end) noexcept
    {
        if (begin > end)
        {
            std::swap(begin, end);
        }
        begin = std::min(begin, capacity_);
        end = std::min(end, capacity_);
        return {storage_.get() + begin, storage_.get() + end};
    }

  private:
    static constexpr std::size_t kDefaultBase = 0;

    std::size_t capacity_;
    std::size_t max_capacity_;
    // base: initially, where the prependable region ends and the appendable region begins [0,
    // capacity_] if base_ == 0, the prependable region is empty initially if base_ == capacity_,
    // the appendable region is empty initially
    std::unique_ptr<char[]> storage_;
    // Invariant: begin_ <= end_ <= storage_.size()
    std::size_t begin_{0};
    std::size_t end_{0};
};
} // namespace Foundation::Core
