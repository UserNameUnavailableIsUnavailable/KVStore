#pragma once

#include <map>
#include <memory_resource>

namespace KV
{
template <typename K, typename V>
class RedBlackTreeMap
{
    using Underlying = std::pmr::map<K, V>;

public:
    using iterator = typename Underlying::iterator;

    explicit RedBlackTreeMap(std::pmr::memory_resource* resource) :
        container_(resource)
    {
    }

    iterator Begin()
    {
        return container_.begin();
    }

    iterator End()
    {
        return container_.end();
    }

    template <typename _K, typename _V>
    requires std::constructible_from<K, _K&&> && std::constructible_from<V, _V&&>
    iterator Insert(_K&& key, _V&& v)
    {
        return container_.insert_or_assign(
            std::forward<_K>(key),
            std::forward<_V>(v)
        ).first;
    }
    
    template <typename _K>
    requires std::constructible_from<K, _K>
    iterator Find(const _K& key)
    {
        return container_.find(key);
    }
    
    void Erase(iterator it)
    {
        container_.erase(it);
    }

    template <typename _K>
    requires std::constructible_from<K, _K>
    void Erase(const _K& key)
    {
        container_.erase(key);
    }

private:
    Underlying container_;
};
} // namespace KV
