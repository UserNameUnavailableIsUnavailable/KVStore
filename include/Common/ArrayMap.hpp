#pragma once

#include <vector>
#include <memory_resource>
#include <algorithm>

namespace KV
{
template <typename K, typename V>
class ArrayMap
{
    using Underlying = std::pmr::vector<std::pair<K, V>>;

public:
    using iterator = typename Underlying::iterator;

    explicit ArrayMap(std::pmr::memory_resource* resource) :
        container_(resource)
    {
    }

    template <typename _K>
    requires std::constructible_from<K, _K>
    iterator Find(const _K& key)
    {
        return std::find_if(container_.begin(), container_.end(),
            [&key](const auto& entry) { return entry.first == key; });
    }

    iterator End()
    {
        return container_.end();
    }
    
    template <typename _K, typename _V>
    requires std::constructible_from<K, _K&&> && std::constructible_from<V, _V&&>
    iterator Insert(_K&& key, _V&& mapped)
    {
        container_.emplace_back(std::forward<_K>(key), std::forward<_V>(mapped));
        return std::prev(container_.end());
    }
    
    void Erase(iterator it)
    {
        container_.erase(it);
    }
    
    template <typename _K>
    requires std::constructible_from<K, _K>
    void Erase(const _K& key)
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