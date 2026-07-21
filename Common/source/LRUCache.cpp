#include "Common/LRUCache.hpp"
#include <optional>

namespace KV
{
template <typename V, typename Rep>
bool LRUCache<V, Rep>::Exists(std::string_view key)
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

template <typename V, typename Rep>
std::optional<V> LRUCache<V, Rep>::Get(std::string_view key)
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

template <typename V, typename Rep>
LRUCacheStatus LRUCache<V, Rep>::Set(std::string_view key, std::optional<V> value)
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
			node->Update(value);
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
		auto back = records_.end() - 1;
		// the last element is invalid
		if (!back->IsValid() || records_.size() == capacity_)
		{
			// if the last element is invalid, replace it
			map_.erase(key);
			back->Update(key, value);
			map_[key] = back;
		}
		else
		{
			records_.emplace_back(key, value.value());
		}
	}
	return status;
}

} // KV
