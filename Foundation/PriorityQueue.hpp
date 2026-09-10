#pragma once

#include <algorithm>
#include <vector>

namespace Foundation
{
template <typename T, typename Compare = std::less<T>> class PriorityQueue
{
  public:
    void push(const T &value)
    {
        data_.push_back(value);
        std::push_heap(data_.begin(), data_.end(), comparator_);
    }

    template <typename... VA> void emplace(VA &&...va)
    {
        data_.emplace_back(std::forward<VA>(va)...);
        std::push_heap(data_.begin(), data_.end(), comparator_);
    }

    void pop()
    {
        std::pop_heap(data_.begin(), data_.end(), comparator_);
        data_.pop_back();
    }

    const T &top() const
    {
        return data_.front();
    }

    bool is_empty() const
    {
        return data_.empty();
    }
    std::size_t size() const
    {
        return data_.size();
    }

    bool remove(const T &value)
    {
        auto it = std::find(data_.begin(), data_.end(), value);
        if (it == data_.end())
            return false;

        // Swap with last and remove
        std::iter_swap(it, data_.end() - 1);
        data_.pop_back();

        // Rebuild heap (O(n))
        std::make_heap(data_.begin(), data_.end(), comparator_);
        return true;
    }

    template <typename Predicate> bool remove_if(Predicate pred)
    {
        auto it = std::find_if(data_.begin(), data_.end(), pred);
        if (it == data_.end())
            return false;

        std::iter_swap(it, data_.end() - 1);
        data_.pop_back();
        std::make_heap(data_.begin(), data_.end(), comparator_);
        return true;
    }

    template <typename Predicate> size_t remove_all_if(Predicate pred)
    {
        auto new_end = std::remove_if(data_.begin(), data_.end(), pred);
        size_t removed = data_.end() - new_end;
        if (removed > 0)
        {
            data_.erase(new_end, data_.end());
            std::make_heap(data_.begin(), data_.end(), comparator_);
        }
        return removed;
    }

    const std::vector<T> &data() const
    {
        return data_;
    }

  private:
    std::vector<T> data_;
    Compare comparator_;
};
} // namespace Foundation
