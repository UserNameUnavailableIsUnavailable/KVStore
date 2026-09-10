#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory_resource>
#include <random>
#include <utility>

namespace KV
{
namespace
{
template <std::size_t LevelCount, typename K, typename V> struct SkipListNode
{
    using NodeType = SkipListNode<LevelCount, K, V>;

    SkipListNode() : key(), value(), forward{}
    {
    }

    SkipListNode(K &&key, V &&value) : key(std::forward<K>(key)), value(std::forward<V>(value)), forward{}
    {
    }

    K key;
    V value;
    std::array<NodeType *, LevelCount> forward;
};
} // namespace

template <float Probability>
concept IsValidProbability = Probability > 0.0f && Probability < 1.0f;

template <typename K, typename V, std::size_t LevelCount = 64, float Probability = 0.5f>
    requires(IsValidProbability<Probability> && LevelCount > 0)
class SkipListMap
{
    using NodeType = SkipListNode<LevelCount, K, V>;

  public:
    class Iterator
    {
      public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<const K, V>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type *;
        using reference = value_type &;

        explicit Iterator(NodeType *ptr = nullptr) : ptr_(ptr)
        {
        }

        std::pair<const K &, V &> operator*() const
        {
            return {ptr_->key, ptr_->value};
        }
        NodeType *operator->() const noexcept
        {
            return ptr_;
        }
        Iterator &operator++() noexcept
        {
            if (ptr_)
            {
                ptr_ = ptr_->forward[0];
            }
            return *this;
        }
        Iterator &operator++(int) noexcept
        {
            Iterator ret = *this;
            if (ptr_)
            {
                ptr_ = ptr_->forward[0];
            }
            return *this;
        }
        friend bool operator==(const Iterator &a, const Iterator &b) noexcept
        {
            return a.ptr_ == b.ptr_;
        }
        friend bool operator!=(const Iterator &a, const Iterator &b) noexcept
        {
            return a.ptr_ != b.ptr_;
        }

      private:
        NodeType *ptr_;
    };

    using ConstIterator = const Iterator;
    using KeyType = K;
    using ValueType = V;

    explicit SkipListMap(std::pmr::memory_resource *memory_resource = std::pmr::get_default_resource())
        : memory_resource_(memory_resource), random_engine_(std::random_device{}())
    {
    }
    SkipListMap(const SkipListMap &) = delete;
    SkipListMap &operator=(const SkipListMap &) = delete;

    ~SkipListMap()
    {
        auto cur = sentinel_.forward[0];
        if (cur)
        {
            auto forward = cur->forward[0];
            destroy_node(cur);
            cur = forward;
        }
    }

    void ReSeed() const
    {
        random_engine_ = std::mt19937(std::random_device{}());
    }

    // We need _K and _V to allow implicit construction of K and V.
    // std::constructible_from is used to ensure that _K and _V can be converted
    // to K&& and V&&, where `&&` ensures as-is forwarding of types.

    template <typename _K, typename _V>
        requires std::constructible_from<K, _K &&> && std::constructible_from<V, _V &&>
    Iterator insert(_K &&key, _V &&value)
    {
        std::ptrdiff_t level = static_cast<std::ptrdiff_t>(highest_level_index_);
        auto it = &sentinel_;

        auto forward = [&level](NodeType *it) { return it->forward[level]; };

        std::array<NodeType *, LevelCount> prevs;
        prevs.fill(&sentinel_);

        while (level >= 0)
        {
            auto next = forward(it);
            while (next)
            {
                if (next->key < key)
                {
                    it = next;
                    next = forward(it);
                }
                else if (next->key == key)
                {
                    it = next;
                    it->value = V(std::forward<_V>(value));
                    return Iterator(it);
                }
                else // the next key is larger, stop
                {
                    break;
                }
            }
            prevs[level] = it;
            level--;
        }

        it = create_node(K(std::forward<_K>(key)), V(std::forward<_V>(value)));
        level = static_cast<std::ptrdiff_t>(random_level());
        highest_level_index_ = std::max(highest_level_index_, static_cast<std::size_t>(level));
        while (level >= 0)
        {
            auto prev = prevs[level];
            it->forward[level] = prev->forward[level];
            prev->forward[level] = it;
            level--;
        }
        size_++;
        return Iterator(it);
    }

    template <typename _K>
        requires std::constructible_from<K, _K>
    Iterator find(const _K &key)
    {
        auto level = static_cast<std::ptrdiff_t>(highest_level_index_);
        auto it = &sentinel_;
        auto forward = [&level](NodeType *it) { return it->forward[level]; };
        while (level >= 0)
        {
            auto next = forward(it);
            while (next)
            {
                if (next->key < key)
                {
                    it = next;
                    next = forward(it);
                }
                else if (next->key == key)
                {
                    it = next;
                    return Iterator(next);
                }
                else // find a larger key, stop
                {
                    break;
                }
            }
            level--;
        }
        return Iterator(nullptr);
    }

    template <typename _K>
        requires std::constructible_from<K, _K>
    ConstIterator find(const _K &key) const
    {
        return find(key);
    }

    template <typename _K>
        requires std::constructible_from<K, _K>
    std::size_t erase(const _K &key)
    {
        if (size_ == 0) [[unlikely]]
        {
            return 0;
        }
        NodeType *it = &sentinel_;
        std::ptrdiff_t level = highest_level_index_;
        auto forward = [&level](NodeType *it) { return it->forward[level]; };
        std::array<NodeType *, LevelCount> predecessors;
        predecessors.fill(nullptr); // we delete a node via its predecessors
        while (level >= 0)
        {
            auto next = forward(it);
            while (next)
            {
                if (next->key < key)
                {
                    it = next;
                    next = forward(it);
                }
                else if (next->key == key)
                {
                    predecessors[level] = it;
                    break;
                }
                else // not found in the current level
                {
                    break;
                }
            }
            level--;
        }
        level = highest_level_index_;

        it = nullptr;             // the node to be deleted
        NodeType *prev = nullptr; // the predecessor
        while (level >= 0)
        {
            auto prev = predecessors[level];
            if (prev)
            {
                it = forward(prev);
                prev->forward[level] = it->forward[level];
            }
            level--;
        }
        // update the highest level
        while (highest_level_index_ > 0 && sentinel_.forward[highest_level_index_] == nullptr)
        {
            highest_level_index_--;
        }
        // delete the node at level 0
        if (it != nullptr)
        {
            destroy_node(it);
            size_--;
            return 1;
        }
        return 0;
    }

    bool IsEmpty() const noexcept
    {
        return sentinel_.forward[0] == nullptr;
    }
    Iterator begin() noexcept
    {
        return Iterator(sentinel_.forward[0]);
    }
    ConstIterator begin() const noexcept
    {
        return ConstIterator(sentinel_.forward[0]);
    }
    Iterator end() noexcept
    {
        return Iterator(nullptr);
    }
    ConstIterator end() const noexcept
    {
        return ConstIterator(nullptr);
    }

  private:
    NodeType *create_node()
    {
        return new NodeType;
    }

    NodeType *create_node(K &&key, V &&value)
    {
        NodeType *node = new NodeType(std::forward<K>(key), std::forward<V>(value));
        return node;
    }

    void destroy_node(NodeType *node) noexcept
    {
        delete node;
    }

    void clear() noexcept
    {
        NodeType *current = sentinel_.forward[0];
        while (current != nullptr)
        {
            NodeType *next = current->forward[0];
            destroy_node(current);
            current = next;
        }
        sentinel_.forward.fill(nullptr);
    }

    std::size_t random_level() const
    {
        if constexpr (is_level_randomization_acceleratable())
        {
            return random_level_fast();
        }
        else
        {
            return random_level_slow();
        }
    }

    std::size_t random_level_slow() const
    {
        std::size_t level = 0;
        std::uniform_real_distribution<float> distribution(0.0f, 1.0f);
        while (level < LevelCount - 1 && distribution(random_engine_) < Probability)
        {
            ++level;
        }
        return level;
    }

    std::size_t random_level_fast() const
    {
        constexpr std::uint32_t bits = std::bit_cast<std::uint32_t>(Probability);
        constexpr unsigned exponent = (bits >> 23u) & 0xffu;
        constexpr unsigned successive_ones = 127u - exponent;
        constexpr std::uint32_t mask = std::numeric_limits<std::uint32_t>::max() >> (32u - successive_ones);

        std::size_t level = 0;
        while (level < LevelCount)
        {
            const std::uint32_t random_bits = random_engine_() & mask;
            if (std::popcount(random_bits) != successive_ones)
            {
                break;
            }
            ++level;
        }
        return level == LevelCount ? static_cast<std::size_t>(LevelCount - 1) : level;
    }

    // Fast generation is valid for probabilities exactly equal to 2^-n,
    // 1 <= n <= 32. The bit test is constexpr and avoids std::frexp, which is
    // not constexpr on the supported standard library.
    static constexpr bool is_level_randomization_acceleratable() noexcept
    {
        constexpr std::uint32_t bits = std::bit_cast<std::uint32_t>(Probability);
        constexpr std::uint32_t fraction_mask = (1u << 23u) - 1u;
        constexpr unsigned exponent = (bits >> 23u) & 0xffu;
        return (bits & fraction_mask) == 0 && exponent >= 95 && exponent <= 126;
    }

    NodeType sentinel_;
    std::pmr::memory_resource *memory_resource_;
    mutable std::mt19937 random_engine_;
    std::size_t size_ = 0;
    std::size_t highest_level_index_ = 0;
};
} // namespace KV
