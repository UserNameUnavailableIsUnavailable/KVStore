#pragma once

#include <chrono>
#include <cstddef>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <unordered_map>
#include <utility>

#include "Common/Record.hpp"
#include "Common/ArrayMap.hpp"
#include "Common/HashMap.hpp"
#include "Common/RedBlackTreeMap.hpp"
#include "Common/SkipListMap.hpp"

namespace KV
{
// -------- DataStore --------
// The layer that both stores and indexes records. It is abstract: a cache
// strategy is not a parameter of a store, it *is* the store type. Concrete
// stores (LRUDataStore, LFUDataStore, ...) implement Get / Set / Exists and in
// doing so spell out their policy directly.
//
//   - Storing : records live in a std::pmr::list, so insertion and removal are
//               O(1) and, crucially, iterators stay valid across every mutation
//               (including splice). That iterator is the handle.
//   - Indexing: an injected Container maps a key to that handle. Any container
//               exposing the adapter interface below works, which is how the
//               array / red-black tree / hash / skip-list variants are chosen:
//
//                 using iterator;                      // yields it->second == handle
//                 iterator Find(const Key&);
//                 iterator End();
//                 iterator Insert(const Key&, Mapped); // key assumed absent
//                 void     Erase(iterator);
//                 void     Erase(const Key&);
//
// What this base owns is the machinery every strategy needs and none should
// reimplement: node recycling, keeping the index and the record list in step,
// firing the lifecycle transitions, and the record order primitives. What it
// deliberately does *not* own is any decision: it never removes a record behind
// a derived store's back (not even an expired one), so a strategy with its own
// bookkeeping - LFU's frequency buckets, say - can keep that bookkeeping exact
// simply by routing every removal through its own code.
//
template <typename KeyType, typename ValueType,
    template <typename, typename> class Container = HashMap>
class DataStore
{
public:
    using RecordType = Record<KeyType, ValueType>;
    using RecordList = std::pmr::list<RecordType>;
    // A handle is a list iterator: stable for the whole life of the record, so
    // the index never has to be patched up after a reorder or a removal.
    using RecordHandle = typename RecordList::iterator;

    // capacity == 0 means unbounded (never evicts).
    explicit DataStore(std::size_t capacity = 0,
        std::pmr::memory_resource* resource = std::pmr::get_default_resource()) :
        capacity_(capacity),
        resource_(resource),
        records_(resource),
        recycled_(resource),
        index_(resource)
    {
    }

    virtual ~DataStore() = default;

    // The index stores handles into records_, so a copy would have its handles
    // pointing at the original's records. Moving a polymorphic store risks
    // slicing, so neither is allowed: a store stays where it was built.
    DataStore(const DataStore&) = delete;
    DataStore& operator=(const DataStore&) = delete;
    DataStore(DataStore&&) = delete;
    DataStore& operator=(DataStore&&) = delete;

    // -------- The strategy's interface to the world --------

    virtual std::optional<ValueType> Get(const KeyType& key) = 0;

    // value == std::nullopt deletes the key.
    virtual void Set(const KeyType& key, std::optional<ValueType> value) = 0;

    virtual bool Exists(const KeyType& key) = 0;

    std::size_t Size() const noexcept { return records_.size(); }
    std::size_t GetCapacity() const noexcept { return capacity_; }
    bool IsFull() const noexcept { return capacity_ != 0 && records_.size() >= capacity_; }

protected:

    // -------- Machinery a concrete store builds on --------

    std::pmr::memory_resource* GetMemoryResource() const noexcept { return resource_; }

    // Pure lookup: no lifecycle transition, no reordering, no removal. Notably
    // an expired record is still returned, because dropping it is a decision
    // and decisions belong to the derived store.
    RecordHandle Find(const KeyType& key)
    {
        const auto it = index_.Find(key);
        return it == index_.End() ? NoRecord() : (*it).second;
    }

    // Insert a fresh record at the front (newest) and index it. A recycled node
    // is reused when one is available, so a steady-state store stops allocating
    // list nodes and reuses the key/value buffers those nodes still hold.
    RecordHandle Create(const KeyType& key, ValueType value)
    {
        RecordHandle handle;
        if (recycled_.empty())
        {
            records_.emplace_front(key, std::move(value));
            handle = records_.begin();
        }
        else
        {
            handle = recycled_.begin();
            *handle = RecordType(key, std::move(value)); // fresh lifecycle
            records_.splice(records_.begin(), recycled_, handle);
        }

        index_.Insert(key, handle);
        return handle;
    }

    // Unindex a record and park its node on the free list. Every removal -
    // delete, expiry, eviction - funnels through here, and it is always the
    // derived store that calls it.
    void Remove(RecordHandle handle)
    {
        if (handle == NoRecord())
        {
            return;
        }
        index_.Erase(handle->GetKey());
        recycled_.splice(recycled_.begin(), records_, handle);
    }

    // Record order. For list-ordered strategies (LRU, FIFO, MRU) this order is
    // the policy; strategies with their own structures can ignore it entirely.
    void Promote(RecordHandle handle) { records_.splice(records_.begin(), records_, handle); }
    void Demote(RecordHandle handle) { records_.splice(records_.end(), records_, handle); }
    RecordHandle Newest() { return records_.begin(); }
    RecordHandle Oldest() { return records_.empty() ? NoRecord() : std::prev(records_.end()); }
    RecordList& Records() noexcept { return records_; }
    RecordHandle NoRecord() { return records_.end(); }

private:
    std::size_t capacity_ = 0;
    std::pmr::memory_resource* resource_ = std::pmr::get_default_resource();
    RecordList records_; // live records, kept in strategy-defined order
    RecordList recycled_; // removed nodes, reused on creation
    Container<KeyType, RecordHandle> index_; // key -> handle
};

// -------- LRU --------
// Least-Recently-Used: every use moves the record to the front of the record
// list, so the back is always the least recently used record and thus the
// victim. The list *is* the whole strategy - no extra state at all.
template <typename KeyType, typename ValueType,
    template <typename, typename> class Container = HashMap>
class LRUDataStore : public DataStore<KeyType, ValueType, Container>
{
    using Base = DataStore<KeyType, ValueType, Container>;

public:
    using Base::Base;
    using RecordHandle = typename Base::RecordHandle;

    std::optional<ValueType> Get(const KeyType& key) override
    {
        const RecordHandle handle = Resolve(key);
        if (handle == this->NoRecord())
        {
            return std::nullopt;
        }
        this->Promote(handle);
        return handle->GetValue();
    }

    void Set(const KeyType& key, std::optional<ValueType> value) override
    {
        const RecordHandle handle = Resolve(key);
        if (handle != this->NoRecord())
        {
            if (!value.has_value()) // delete
            {
                this->Remove(handle);
                return;
            }
            handle->SetValue(std::move(*value));
            this->Promote(handle); // a write is a use
            return;
        }

        if (!value.has_value())
        {
            return; // deleting a missing key is a no-op
        }

        if (this->IsFull())
        {
            this->Remove(this->Oldest()); // the least recently used record
        }
        this->Create(key, std::move(*value)); // lands at the front, i.e. newest
    }

    bool Exists(const KeyType& key) override
    {
        const RecordHandle handle = Resolve(key);
        if (handle == this->NoRecord())
        {
            return false;
        }
        this->Promote(handle);
        return true;
    }

    template <typename Rep, typename Period>
    bool SetTimeToLive(const KeyType& key, std::chrono::duration<Rep, Period> ttl)
    {
        const RecordHandle handle = Resolve(key);
        if (handle == this->NoRecord())
        {
            return false;
        }
        handle->SetTimeToLive(std::optional {ttl});
        return true;
    }

    std::optional<std::chrono::milliseconds> GetTimeToLive(const KeyType& key)
    {
        const RecordHandle handle = Resolve(key);
        if (handle == this->NoRecord())
        {
            return std::nullopt;
        }
        if (const auto expiry = handle->GetExpiryTimepoint(); expiry.has_value())
        {
            return std::chrono::ceil<std::chrono::milliseconds>(*expiry - std::chrono::system_clock::now());
        }
        return std::nullopt;
    }

    template <typename F>
    void VisitLive(F&& visitor)
    {
        for (const auto& record : this->Records())
        {
            if (!record.IsAlive())
            {
                continue;
            }
            std::optional<std::chrono::milliseconds> ttl;
            if (const auto expiry = record.GetExpiryTimepoint(); expiry.has_value())
            {
                ttl = std::chrono::ceil<std::chrono::milliseconds>(*expiry - std::chrono::system_clock::now());
            }
            visitor(record.GetKey(), record.GetValue(), ttl);
        }
    }

private:
    // Lazy expiry: a dead record is dropped here, so nothing above ever sees one.
    RecordHandle Resolve(const KeyType& key)
    {
        const RecordHandle handle = this->Find(key);
        if (handle != this->NoRecord() && !handle->IsAlive())
        {
            this->Remove(handle);
            return this->NoRecord();
        }
        return handle;
    }
};

// -------- LFU --------
// Least-Frequently-Used, with frequency buckets: a map from use count to the
// handles having that count, most recently used at the front of each bucket.
// The lowest-count bucket is therefore always buckets_.begin(), and its back is
// the victim (ties broken by least recently used).
//
// These buckets are the store's own private bookkeeping, and because every
// removal goes through this class's Drop(), they can never fall out of sync
// with the records they describe.
template <typename KeyType, typename ValueType,
    template <typename, typename> class Container = HashMap>
class LFUDataStore : public DataStore<KeyType, ValueType, Container>
{
    using Base = DataStore<KeyType, ValueType, Container>;

public:
    using RecordHandle = typename Base::RecordHandle;

    explicit LFUDataStore(std::size_t capacity = 0,
        std::pmr::memory_resource* resource = std::pmr::get_default_resource()) :
        Base(capacity, resource),
        buckets_(resource),
        entries_(resource)
    {
    }

    std::optional<ValueType> Get(const KeyType& key) override
    {
        const RecordHandle handle = Resolve(key);
        if (handle == this->NoRecord())
        {
            return std::nullopt;
        }
        Bump(key);
        return handle->GetValue();
    }

    void Set(const KeyType& key, std::optional<ValueType> value) override
    {
        const RecordHandle handle = Resolve(key);
        if (handle != this->NoRecord())
        {
            if (!value.has_value()) // delete
            {
                Drop(handle);
                return;
            }
            handle->SetValue(std::move(*value));
            Bump(key); // a write is a use
            return;
        }

        if (!value.has_value())
        {
            return; // deleting a missing key is a no-op
        }

        if (this->IsFull())
        {
            Drop(Victim());
        }
        Admit(this->Create(key, std::move(*value)));
    }

    bool Exists(const KeyType& key) override
    {
        const RecordHandle handle = Resolve(key);
        if (handle == this->NoRecord())
        {
            return false;
        }
        Bump(key);
        return true;
    }

    // Use count of a key, or 0 if it is not stored.
    std::size_t FrequencyOf(const KeyType& key) const
    {
        const auto it = entries_.find(key);
        return it == entries_.end() ? 0 : it->second.frequency;
    }

private:
    using HandleList = std::pmr::list<RecordHandle>;
    struct Entry
    {
        std::size_t frequency = 0;
        typename HandleList::iterator position; // where the handle sits in its bucket
    };

    // Lazy expiry, routed through Drop so the buckets stay exact.
    RecordHandle Resolve(const KeyType& key)
    {
        const RecordHandle handle = this->Find(key);
        if (handle != this->NoRecord() && !handle->IsAlive())
        {
            Drop(handle);
            return this->NoRecord();
        }
        return handle;
    }

    // Remove a record from the store and from the frequency bookkeeping.
    void Drop(RecordHandle handle)
    {
        if (handle == this->NoRecord())
        {
            return;
        }
        Forget(handle->GetKey());
        this->Remove(handle);
    }

    // A new record enters with a use count of one.
    void Admit(RecordHandle handle)
    {
        HandleList& bucket = buckets_[1];
        bucket.push_front(handle);
        entries_.insert_or_assign(handle->GetKey(), Entry {.frequency = 1, .position = bucket.begin()});
    }

    // Move a handle from its bucket to the next one up.
    void Bump(const KeyType& key)
    {
        const auto it = entries_.find(key);
        if (it == entries_.end())
        {
            return;
        }

        Entry& entry = it->second;
        const RecordHandle handle = *entry.position;
        EraseFromBucket(entry.frequency, entry.position);

        ++entry.frequency;
        HandleList& bucket = buckets_[entry.frequency];
        bucket.push_front(handle);
        entry.position = bucket.begin();
    }

    void Forget(const KeyType& key)
    {
        const auto it = entries_.find(key);
        if (it == entries_.end())
        {
            return;
        }
        EraseFromBucket(it->second.frequency, it->second.position);
        entries_.erase(it);
    }

    void EraseFromBucket(std::size_t frequency, typename HandleList::iterator position)
    {
        const auto bucket = buckets_.find(frequency);
        if (bucket == buckets_.end())
        {
            return;
        }
        bucket->second.erase(position);
        if (bucket->second.empty())
        {
            buckets_.erase(bucket); // keeps begin() on the lowest live frequency
        }
    }

    // Least frequently used; among equals, the least recently used.
    RecordHandle Victim()
    {
        return buckets_.empty() ? this->NoRecord() : buckets_.begin()->second.back();
    }

    std::pmr::map<std::size_t, HandleList> buckets_; // frequency -> handles, MRU at the front
    std::pmr::unordered_map<KeyType, Entry> entries_; // key -> {frequency, position in bucket}
};
} // namespace KV
