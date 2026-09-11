#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>

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

    std::size_t prependable_size() const noexcept
    {
        return begin_;
    }

    std::span<const char> prependable_span() const noexcept
    {
        return {storage_.get(), storage_.get() + begin_};
    }

    std::size_t appendable_size() const noexcept
    {
        return capacity_ - end_;
    }

    std::span<char> appendable_span() noexcept
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

    std::span<char> valid_span() const noexcept
    {
        return {storage_.get() + begin_, storage_.get() + end_};
    }

    bool reserve_for_prepend(std::size_t size);
    bool reserve_for_append(std::size_t size);
    bool prepend(const char *data, std::size_t size);
    bool prepend(const char *data);
    bool append(const char *data, std::size_t size);
    bool append(const char *data);

    void commit(std::size_t size) noexcept
    {
        assert(size <= appendable_size());
        end_ += size;
    }

    void consume(std::size_t size) noexcept;
    void consume_all() noexcept
    {
        begin_ = end_ = base_;
    }

    std::size_t capacity() const noexcept
    {
        return capacity_;
    }

    void clear() noexcept
    {
        begin_ = base_;
        end_ = base_;
    }

    std::size_t get_base() const noexcept
    {
        return base_;
    }
    // changes the base offset of the buffer
    // if base >= capacity_, the buffer will expand (begin_ & end_ do not change)
    void set_base(std::size_t base);

    // shrink the buffer to the minimum capacity that can hold the data
    void shrink();

    [[nodiscard]] std::size_t valid_size() const noexcept
    {
        return end_ - begin_;
    }

  private:
    static constexpr std::size_t kDefaultBase = 0;

    std::size_t capacity_;
    std::size_t max_capacity_;
    // base: initially, where the prependable region ends and the appendable region begins [0,
    // capacity_] if base_ == 0, the prependable region is empty initially if base_ == capacity_,
    // the appendable region is empty initially
    std::size_t base_;
    std::unique_ptr<char[]> storage_;
    // Invariant: begin_ <= end_ <= storage_.size()
    std::size_t begin_{base_};
    std::size_t end_{base_};
};
} // namespace Foundation::Core
