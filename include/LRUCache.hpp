#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <list>
#include <unordered_map>

namespace KV
{
template <typename V, typename Rep = std::chrono::milliseconds>
struct Record
{
    V value;
    std::chrono::steady_clock::time_point last_accessed;
    std::chrono::duration<Rep> time_to_live;
};

template <typename V>
class LRUCache
{
public:
    LRUCache(std::size_t capacity) : capacity_(capacity)
    {
    }
    V Get(const std::string_view key);
    void Set(const std::string_view key, V&& value);

private:
    std::size_t capacity_;
    std::list<std::pair<std::string, V>> records_; // doubly linked list to maintain the order of usage
    std::unordered_map<std::string, typename std::list<std::pair<std::string, V>>::iterator> map_;
};
}