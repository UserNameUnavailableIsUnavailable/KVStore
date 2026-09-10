#pragma once

#include <map>

namespace KV
{
template <typename K, typename V> class RedBlackTreeMap
{
    using Underlying = std::pmr::map<K, V>;

  public:
    using Iterator = typename Underlying::iterator;
    using ConstIterator = typename Underlying::const_iterator;
    using KeyType = K;
    using ValueType = V;

    explicit RedBlackTreeMap()
    {
    }

    Iterator begin()
    {
        return container_.begin();
    }

    Iterator end()
    {
        return container_.end();
    }

    template <typename _K, typename _V>
        requires std::constructible_from<K, _K &&> && std::constructible_from<V, _V &&>
    Iterator insert(_K &&key, _V &&v)
    {
        return container_.insert_or_assign(std::forward<_K>(key), std::forward<_V>(v)).first;
    }

    template <typename _K>
        requires std::constructible_from<K, _K>
    Iterator find(const _K &key)
    {
        return container_.find(key);
    }

    void erase(Iterator it)
    {
        container_.erase(it);
    }

    template <typename _K>
        requires std::constructible_from<K, _K>
    void erase(const _K &key)
    {
        container_.erase(key);
    }

  private:
    Underlying container_;
};
} // namespace KV
