#pragma once

#include <chrono>
#include <cstddef>
#include <iterator>
#include <stdexcept>
#include <string>
#include <list>
#include <unordered_map>

namespace KV
{
enum class LRUCacheStatus
{
	kOk,
	kNonexistent,
	kInvalidArgument
};

template <typename V>
class Record
{
public:
	Record(std::string key, V value) :
		key_(std::move(key)),
		value_(std::move(value))
	{
	}
	void KickOut()
	{
		kicked_out_ = true;
	}
	bool IsValid()
	{
		return !kicked_out_;
	}
	const std::string& GetKey()
	{
		return key_;
	}
	const V& GetValue()
	{
		Reinstate();
		return value_;
	}
	void Update(V value)
	{
		value_ = std::move(value);
		Reinstate();
	}
	void Replace(std::string key, V value)
	{
		key_ = std::move(key);
		value_ = std::move(value);
		Reinstate();
	}
private:
	void Reinstate()
	{
		kicked_out_ = false;
	}

private:
	// TODO: Is it possible to let the key in record and the key in map refer to the same object?
	std::string key_;
    V value_;
	bool kicked_out_ = false;
	// TODO: enable TTL
    std::chrono::steady_clock::time_point last_accessed_;
	std::chrono::time_point<std::chrono::system_clock> expires_at_;
};

template <typename V>
class LRUCache
{
	using RecordType = Record<V>;
	using ListType = std::list<RecordType>;
	using ListNode = ListType::iterator;
public:
    LRUCache(std::size_t capacity) : capacity_(capacity)
    {
    }
    bool Exists(std::string key);
    std::optional<V> Get(std::string key);
    LRUCacheStatus Set(std::string key, std::optional<V> value);
private:
    std::size_t capacity_;
    ListType records_; // doubly linked list to maintain the order of usage
    std::unordered_map<std::string, ListNode> map_;
};

template <typename V>
bool LRUCache<V>::Exists(std::string key)
{
	if (!map_.contains(key))
	{
		return false;
	}
	auto node = map_[key];
	if (!node->IsValid())
	{
		map_.erase(key);
		records_.splice(records_.end(), records_, node);
		return false;
	}
	records_.splice(records_.begin(), records_, node);
	return true;
}

template <typename V>
std::optional<V> LRUCache<V>::Get(std::string key)
{
	if (!map_.contains(key))
	{
		return std::nullopt;
	}
	auto node = map_[key];
	if (!node->IsValid())
	{
		map_.erase(key);
		records_.splice(records_.end(), records_, node);
		return std::nullopt;
	}
	records_.splice(records_.begin(), records_, node);
	return node->GetValue();
}

template <typename V>
LRUCacheStatus LRUCache<V>::Set(std::string key, std::optional<V> value)
{
	auto status = LRUCacheStatus::kOk;
	if (map_.contains(key)) // modify
	{
		auto node = map_[key];
		if (!value.has_value()) // delete
		{
			// erase the key in map
			map_.erase(key);
			// move node to the end of the list
			records_.splice(records_.end(), records_, node);
		}
		else // modify
		{
			node->Update(value.value());
			// move node to the begin of the list
			records_.splice(records_.begin(), records_, node);
		}
	}
	else // create
	{
		if (!value.has_value()) [[unlikely]] // assign null
		{
			return LRUCacheStatus::kInvalidArgument;
		}
		if (records_.empty()) [[unlikely]]
		{
			records_.emplace_front(key, value.value());
			map_[key] = records_.begin();
			return LRUCacheStatus::kOk;
		}
		auto back = std::prev(records_.end());
		// the last element is invalid
		if (!back->IsValid() || records_.size() == capacity_)
		{
			// if the last element is invalid, replace it
			map_.erase(back->GetKey());
			back->Replace(key, value.value());
			map_[key] = back;
			records_.splice(records_.begin(), records_, back);
		}
		else
		{
			records_.emplace_front(key, value.value());
			map_[key] = records_.begin();
		}
	}
	return status;
}
} // namespace KV
