#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <ratio>
#include <system_error>
#include <utility>

namespace KV
{
// A record is a key, a value, and a lifecycle. The lifecycle is held through a
// pointer to the abstract Lifecycle, so a store can hand out records with
// different validity rules (plain, TTL, ...) without Record being templated on
// them. See Lifecycle.hpp for the interface.
//
// Records are internal to the storage layer. Lifecycle transitions (OnRead /
// OnWrite) are driven by the owning store; users of the store never touch them,
// which keeps lifecycle management transparent. A record is meaningful outside
// a container too: it is born alive and dies only when its lifecycle says so
// (e.g. a TTL expiring), independent of any eviction decision.
//
// The key is stored alongside the value because removing a record - on
// deletion, expiry or eviction - requires looking the key back up in the index.

// There are few things that can be improved:
// For POD data, we make value atomic.
// Otherwise, we use std::atomic<std::unique_ptr<V>>.

template <typename K, typename V>
class Record
{
public:
    template <typename _K, typename _V>
    Record(_K&& key, _V&& value) :
        key_(std::forward<_K>(key)),
        value_(std::forward<_V>(value))
    {
    }

    const K& GetKey() const noexcept { return key_; }
    const V& GetValue() const noexcept { return value_; }
	
    void SetValue(V value)
	{
		// Note that KVStore is single-threaded, so there is no risk of
		// data race.
		value_ = std::move(value);
		version_++;
	}

    bool IsAlive() const
	{
		auto now = std::chrono::system_clock::now();
		if (expiry_time_point_.has_value() && *expiry_time_point_ <= now)
		{
			return false;
		}
		return true;
	}

	template <typename Rep, typename Period = std::ratio<1>>
	void SetTimeToLive(std::optional<std::chrono::duration<Rep, Period>> ttl)
	{
		if (ttl.has_value())
		{
			expiry_time_point_.emplace(std::chrono::system_clock::now() + *ttl);
		}
		else
		{
			expiry_time_point_.reset();
		}
	}

	template <typename Rep, typename Period = std::ratio<1>>
	std::optional<std::chrono::duration<Rep, Period>> GetTimeToLive() const
	{
		if (!expiry_time_point_.has_value())
		{
			return std::nullopt;
		}
		return *expiry_time_point_ - std::chrono::system_clock::now();
	}
	
	std::optional<std::chrono::system_clock::time_point> GetExpiryTimepoint() const
	{
		return expiry_time_point_;
	}

private:
    K key_;
    V value_;
	size_t version_; // version number marks the modification of the value
	std::optional<std::chrono::system_clock::time_point> expiry_time_point_;
};
} // namespace KV
