#pragma once

#include <chrono>
#include <utility>

namespace KV
{
template <typename KeyType, typename ValueType> class Record
{
  public:
    const KeyType &get_key() const noexcept
    {
        return key;
    }

    const ValueType &get_value() const noexcept
    {
        return value;
    }

    void set_value(ValueType value) noexcept
    {
        this->value = std::move(value);
        modified_at = accessed_at = std::chrono::steady_clock::now();
    }

    void clear_ttl() noexcept
    {
        has_ttl = false;
    }

    void set_ttl(std::chrono::steady_clock::duration duration) noexcept
    {
        has_ttl = true;
        expired_at = std::chrono::steady_clock::now() + duration;
    }
    
    std::optional<std::chrono::steady_clock::duration> get_ttl() noexcept
    {
        if (!has_ttl) return {};
        auto now = std::chrono::steady_clock::now();
        if (now >= expired_at)
        {
            return {};
        }
        return expired_at - now;
    }

    bool is_alive() const noexcept
    {
        return !has_ttl || std::chrono::steady_clock::now() < expired_at;
    }

    Record(const KeyType &key, ValueType value)
        : key(key), value(std::move(value)), created_at(std::chrono::steady_clock::now()), modified_at(created_at),
          accessed_at(created_at)
    {
    }

  private:
    KeyType key;
    ValueType value;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point modified_at;
    std::chrono::steady_clock::time_point accessed_at;
    bool has_ttl{false};
    std::chrono::steady_clock::time_point expired_at;
};
} // namespace KV