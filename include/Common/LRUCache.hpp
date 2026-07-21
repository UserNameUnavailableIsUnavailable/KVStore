#pragma once

#include <chrono>
#include <cstddef>
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

template <typename V, typename Rep = std::chrono::milliseconds>
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
		return kicked_out_;
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
		value_ = std::move(value_);
		Reinstate();
	}
	void Update(std::string key, V value)
	{
		if (IsValid())
		{
			throw std::runtime_error("cannot overwrite a valid record");
		}
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
    std::chrono::duration<Rep> time_to_live_;
};

template <typename V, typename Rep = std::chrono::milliseconds>
class LRUCache
{
	using RecordType = Record<V, Rep>;
	using ListType = std::list<RecordType>;
	using ListNode = ListType::iterator;
public:
    LRUCache(std::size_t capacity) : capacity_(capacity)
    {
    }
    bool Exists(const std::string_view key);
    std::optional<V> Get(const std::string_view key);
    LRUCacheStatus Set(const std::string_view key, std::optional<V> value);
private:
    std::size_t capacity_;
    ListType records_; // doubly linked list to maintain the order of usage
    std::unordered_map<std::string, ListNode> map_;
};
}
