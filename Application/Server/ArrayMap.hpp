#pragma once

#include <algorithm>
#include <vector>

namespace KV
{
template <typename K, typename V> class ArrayMap
{
    using Underlying = std::vector<std::pair<K, V>>;

  public:
    using Iterator = typename Underlying::iterator;
    using KeyType = K;
    using ValueType = V;

    ArrayMap() = default;

    template <typename _K>
        requires std::constructible_from<K, _K>
    Iterator find(const _K &key)
    {
        return std::find_if(container_.begin(), container_.end(),
                            [&key](const auto &entry) { return entry.first == key; });
    }

    Iterator end()
    {
        return container_.end();
    }

    template <typename _K, typename _V>
        requires std::constructible_from<K, _K &&> && std::constructible_from<V, _V &&>
    Iterator insert(_K &&key, _V &&mapped)
    {
        container_.emplace_back(std::forward<_K>(key), std::forward<_V>(mapped));
        return std::prev(container_.end());
    }

    void erase(Iterator it)
    {
        container_.erase(it);
    }

    template <typename _K>
        requires std::constructible_from<K, _K>
    void erase(const _K &key)
    {
        const Iterator it = find(key);
        if (it != container_.end())
        {
            container_.erase(it);
        }
    }

  private:
    Underlying container_;
};
} // namespace KV
