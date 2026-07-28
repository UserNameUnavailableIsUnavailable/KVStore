#pragma once

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>

#include "Common/EvictionPolicy.hpp"
#include "Common/Lifecycle.hpp"
#include "Common/Record.hpp"
#include "Common/StorageContainer.hpp"

namespace KV
{
// Underlying data structure used to index keys. The factory turns this runtime
// choice into a concrete, fully-templated CacheStorage instantiation.
enum class StorageStructure
{
    kHash, // std::unordered_map
    kTree, // std::map (red-black tree)
    kArray, // flat std::vector
};

// Which record to evict when the store is full.
enum class EvictionStrategy
{
    kLru,
    kLfu, // not yet implemented
};

// -------- Abstract runtime API --------
// Callers hold this interface and never see the structure/eviction/validation
// template arguments. The single virtual boundary is per-request (Get/Set/
// Exists), not per-record, so it stays off the hot inner loops.
template <typename KeyType, typename ValueType>
class Storage
{
public:
    virtual ~Storage() = default;

    // value == std::nullopt deletes the key.
    virtual void Set(const KeyType& key, std::optional<ValueType> value) = 0;

    // Returning std::optional (a copy) is deliberate:
    //   - a pointer/reference into the store could dangle after eviction;
    //   - a bare value cannot express a miss without an expensive exception.
    virtual std::optional<ValueType> Get(const KeyType& key) = 0;

    virtual bool Exists(const KeyType& key) = 0;
};

// -------- Concrete, header-only, zero-overhead implementation --------
// Three orthogonal axes, all resolved at compile time:
//   - Container: the storage structure wrapping a specific STL container
//                (HashContainer / TreeContainer / ArrayContainer), interacted
//                with purely through its iterator interface.
//   - Eviction : the eviction policy (LruPolicy, ...), the container that owns
//                the records and hands out ElementHandles (EvictionPolicy.hpp).
//   - Lifecycle: the record validity / access-accounting policy, see Lifecycle.hpp.
// Every internal container allocates through the supplied pmr memory resource,
// which is how the memory pool reaches the cache.
//
// Records never escape the Storage: the public API speaks only in keys and
// values. Lifecycle transitions (OnRead / OnWrite) are driven internally here,
// so both the eviction policy and Storage users stay oblivious to them.
template <typename KeyType, typename ValueType,
    template <typename, typename> class Container = HashContainer,
    template <typename> class Eviction = LruPolicy,
    typename Lifecycle = BasicLifecycle>
class CacheStorage final : public Storage<KeyType, ValueType>
{
    using RecordType = Record<KeyType, ValueType, Lifecycle>;
    using Policy = Eviction<RecordType>;
    using ElementHandle = typename Policy::ElementHandle;
    using KeyIndex = Container<KeyType, ElementHandle>;

public:
    // capacity == 0 means unbounded (never evicts).
    explicit CacheStorage(std::size_t capacity,
        std::pmr::memory_resource* resource = std::pmr::get_default_resource()) :
        capacity_(capacity),
        policy_(resource),
        index_(resource)
    {
    }

    void Set(const KeyType& key, std::optional<ValueType> value) override
    {
        const auto it = index_.Find(key);
        if (it != index_.End())
        {
            if (!value.has_value()) // delete
            {
                policy_.Erase(it->second);
                index_.Erase(it);
                return;
            }
            const ElementHandle handle = it->second; // update in place
            RecordType& record = policy_.At(handle);
            record.SetValue(std::move(*value));
            record.OnWrite(); // a write revives an expired/dead record
            policy_.Touch(handle);
            return;
        }
        if (!value.has_value())
        {
            return; // deleting a missing key is a no-op
        }
        EvictIfFull();
        const ElementHandle handle = policy_.Insert(key, std::move(*value));
        policy_.At(handle).OnWrite(); // a fresh record is born alive and written
        index_.Insert(key, handle);
    }

    std::optional<ValueType> Get(const KeyType& key) override
    {
        const auto it = index_.Find(key);
        if (it == index_.End())
        {
            return std::nullopt;
        }
        const ElementHandle handle = it->second;
        RecordType& record = policy_.At(handle);
        if (!record.IsAlive())
        {
            policy_.Erase(handle);
            index_.Erase(it);
            return std::nullopt;
        }
        record.OnRead(); // only fired on a valid record
        policy_.Touch(handle);
        return record.GetValue();
    }

    bool Exists(const KeyType& key) override
    {
        const auto it = index_.Find(key);
        if (it == index_.End())
        {
            return false;
        }
        const ElementHandle handle = it->second;
        RecordType& record = policy_.At(handle);
        if (!record.IsAlive())
        {
            policy_.Erase(handle);
            index_.Erase(it);
            return false;
        }
        record.OnRead(); // only fired on a valid record
        policy_.Touch(handle);
        return true;
    }

private:
    void EvictIfFull()
    {
        if (capacity_ == 0 || policy_.Size() < capacity_)
        {
            return;
        }
        const ElementHandle victim = policy_.Victim();
        index_.Erase(policy_.At(victim).GetKey());
        policy_.Erase(victim);
    }

    std::size_t capacity_;
    Policy policy_;
    KeyIndex index_;
};

// -------- Factory --------
// Creates the concrete CacheStorage for the chosen storage structure and
// eviction strategy and wires in the memory pool. Declared for the KV store's
// string key/value type.
std::unique_ptr<Storage<std::string, std::string>> MakeStorage(
    StorageStructure structure,
    EvictionStrategy eviction,
    std::size_t capacity,
    std::pmr::memory_resource* resource = std::pmr::get_default_resource());
} // namespace KV
