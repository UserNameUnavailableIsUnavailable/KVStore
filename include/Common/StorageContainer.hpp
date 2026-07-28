#pragma once

#include <algorithm>
#include <iterator>
#include <map>
#include <memory_resource>
#include <unordered_map>
#include <utility>
#include <vector>

namespace KV
{
// -------- Storage-container adapters --------
// A container adapter maps a key to the eviction policy's ElementHandle. Every
// adapter wraps a specific STL container and exposes the same iterator-based
// interface so that CacheStorage stays agnostic to the concrete structure:
//
//   using iterator;                                // yields it->first / it->second
//   iterator Find(const Key&);
//   iterator End();
//   iterator Insert(const Key&, Mapped);           // key assumed absent
//   void     Erase(iterator);
//   void     Erase(const Key&);
//
// The iterator is the generalization seam: whether the backing store is a hash
// table, a tree, or a flat array, callers only ever hold and dereference an
// iterator. Each adapter is constructed from a pmr memory resource so that its
// nodes are allocated from the memory pool.

// Hash table (std::unordered_map): average O(1), suited to large data sets.
template <typename Key, typename Mapped>
class HashContainer
{
    using Underlying = std::pmr::unordered_map<Key, Mapped>;

public:
    using iterator = typename Underlying::iterator;

    explicit HashContainer(std::pmr::memory_resource* resource) :
        container_(resource)
    {
    }

    iterator Find(const Key& key) { return container_.find(key); }
    iterator End() { return container_.end(); }
    iterator Insert(const Key& key, Mapped mapped)
    {
        return container_.insert_or_assign(key, mapped).first;
    }
    void Erase(iterator it) { container_.erase(it); }
    void Erase(const Key& key) { container_.erase(key); }

private:
    Underlying container_;
};

// Red-black tree (std::map): ordered, O(log n), a balanced default.
template <typename Key, typename Mapped>
class TreeContainer
{
    using Underlying = std::pmr::map<Key, Mapped>;

public:
    using iterator = typename Underlying::iterator;

    explicit TreeContainer(std::pmr::memory_resource* resource) :
        container_(resource)
    {
    }

    iterator Find(const Key& key) { return container_.find(key); }
    iterator End() { return container_.end(); }
    iterator Insert(const Key& key, Mapped mapped)
    {
        return container_.insert_or_assign(key, mapped).first;
    }
    void Erase(iterator it) { container_.erase(it); }
    void Erase(const Key& key) { container_.erase(key); }

private:
    Underlying container_;
};

// Flat array (std::vector of pairs): linear scan, suited to small data sets.
template <typename Key, typename Mapped>
class ArrayContainer
{
    using Underlying = std::pmr::vector<std::pair<Key, Mapped>>;

public:
    using iterator = typename Underlying::iterator;

    explicit ArrayContainer(std::pmr::memory_resource* resource) :
        container_(resource)
    {
    }

    iterator Find(const Key& key)
    {
        return std::find_if(container_.begin(), container_.end(),
            [&key](const auto& entry) { return entry.first == key; });
    }
    iterator End() { return container_.end(); }
    iterator Insert(const Key& key, Mapped mapped)
    {
        container_.emplace_back(key, mapped);
        return std::prev(container_.end());
    }
    void Erase(iterator it) { container_.erase(it); }
    void Erase(const Key& key)
    {
        const iterator it = Find(key);
        if (it != container_.end())
        {
            container_.erase(it);
        }
    }

private:
    Underlying container_;
};
} // namespace KV
