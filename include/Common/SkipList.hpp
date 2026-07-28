#pragma once

#include <array>
#include <cstddef>
#include <random>
#include <memory_resource>

namespace KV
{
namespace
{
template <typename KeyType, typename ValueType>
struct Element
{
    KeyType key;
    ValueType value;
};

template <std::size_t LevelNumber, typename KeyType, typename ValueType>
struct SkipListNode
{
    using NodeType = SkipListNode<LevelNumber, KeyType, ValueType>;
    Element<KeyType, ValueType> element;
    std::array<NodeType*, LevelNumber> forward;
};
}

template <std::size_t LevelNumber, typename KeyType, typename ValueType>
class SkipListContainer
{
public:
    explicit SkipListContainer(std::pmr::memory_resource* resource = std::pmr::get_default_resource());

    using iterator = SkipListNode<LevelNumber, KeyType, ValueType>*;
    using const_iterator = const SkipListNode<LevelNumber, KeyType, ValueType>*;

    template <typename T>
    std::pair<iterator, bool> Insert(const KeyType& key, T&& value);
    template <typename T>
    std::pair<iterator, bool> Insert(KeyType&& key, T&& value);
    template <typename... VA>
    std::pair<iterator, bool> Emplace(VA&&... va);
    iterator Find(const KeyType& key);
    const_iterator Find(const KeyType& key) const;
    iterator Erase(iterator pos) requires(!std::same_as<const_iterator, iterator>);
    std::size_t Erase(const KeyType& key);
    
    bool IsEmpty() const noexcept { return sentinels_[0]->forward[0] == nullptr; }
    iterator Begin() noexcept { return sentinels_[0]->forward[0]; }
    const_iterator Begin() const noexcept { return sentinels_[0]->forward[0]; }
    iterator End() noexcept { return nullptr; }
    const_iterator End() const noexcept { return nullptr; }

private:
    std::array<iterator, LevelNumber> sentinels_;
    std::size_t RandomizeLevel() const;
    std::random_device random_device_;
    std::mt19937 random_engine_;
    std::uniform_int_distribution<std::size_t> distribution_;
};
} // namespace KV