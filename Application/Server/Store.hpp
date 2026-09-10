#pragma once

#include <chrono>
#include <cstddef>
#include <iterator>
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>

#include "ArrayMap.hpp"
#include "HashMap.hpp"
#include "RedBlackTreeMap.hpp"
#include "SkipList.hpp"

#include "Record.hpp"

namespace KV
{
// The index containers live in namespace Common; bring them in so the default
// template arguments (and users below) can name them unqualified.

template <typename KeyType, typename ValueType, template <typename, typename> class Container = HashMap> class Store
{
  public:
    using RecordType = Record<KeyType, ValueType>;
    using RecordList = std::list<RecordType>;
    // A handle is a list iterator: stable for the whole life of the record, so
    // the index never has to be patched up after a reorder or a removal.
    using RecordHandle = typename RecordList::iterator;

    // capacity == 0 means unbounded (never evicts).
    explicit Store(std::size_t capacity = 0) : capacity_(capacity)
    {
    }

    virtual ~Store() = default;

    // The index stores handles into records_, so a copy would have its handles
    // pointing at the original's records. Moving a polymorphic store risks
    // slicing, so neither is allowed: a store stays where it was built.
    Store(const Store &) = delete;
    Store &operator=(const Store &) = delete;
    Store(Store &&) = delete;
    Store &operator=(Store &&) = delete;

    virtual std::optional<ValueType> get(const KeyType &key) = 0;

    // value == std::nullopt deletes the key.
    virtual void set(const KeyType &key, std::optional<ValueType> value) = 0;

    virtual bool contains(const KeyType &key) = 0;
    virtual bool set_ttl(const KeyType &key, std::chrono::steady_clock::duration ttl) = 0;

    std::size_t size() const noexcept
    {
        return records_.size();
    }
    std::size_t capacity() const noexcept
    {
        return capacity_;
    }
    bool is_full() const noexcept
    {
        return capacity_ != 0 && records_.size() >= capacity_;
    }

    template <typename F> void visit_live(F &&visitor)
    {
        for (auto record = records_.begin(); record != records_.end();)
        {
            if (!record->is_alive())
            {
                const RecordHandle expired = record++;
                discard_expired(expired);
                continue;
            }
            const auto ttl_duration = record->get_ttl();
            const std::optional<std::chrono::milliseconds> ttl =
                ttl_duration ? std::optional{std::chrono::ceil<std::chrono::milliseconds>(*ttl_duration)} : std::nullopt;
            visitor(record->get_key(), record->get_value(), ttl);
            ++record;
        }
    }

  protected:
    // Pure lookup: no lifecycle transition, no reordering, no removal. Notably
    // an expired record is still returned, because dropping it is a decision
    // and decisions belong to the derived store.
    RecordHandle find(const KeyType &key)
    {
        const auto it = index_.find(key);
        return it == index_.end() ? end() : (*it).second;
    }

    // insert a fresh record at the front (newest) and index it. A recycled node
    // is reused when one is available, so a steady-state store stops allocating
    // list nodes and reuses the key/value buffers those nodes still hold.
    RecordHandle create(const KeyType &key, ValueType value)
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

        index_.insert(key, handle);
        return handle;
    }

    // Unindex a record and park its node on the free list. Every removal -
    // delete, expiry, eviction - funnels through here, and it is always the
    // derived store that calls it.
    void remove(RecordHandle handle)
    {
        if (handle == end())
        {
            return;
        }
        index_.erase(handle->get_key());
        recycled_.splice(recycled_.begin(), records_, handle);
    }

    // Record order. For list-ordered strategies (LRU, FIFO, MRU) this order is
    // the policy; strategies with their own structures can ignore it entirely.
    void promote(RecordHandle handle)
    {
        records_.splice(records_.begin(), records_, handle);
    }
    void demote(RecordHandle handle)
    {
        records_.splice(records_.end(), records_, handle);
    }
    RecordHandle front()
    {
        return records_.begin();
    }
    RecordHandle back()
    {
        return records_.empty() ? end() : std::prev(records_.end());
    }
    RecordList &records() noexcept
    {
        return records_;
    }
    RecordHandle end()
    {
        return records_.end();
    }

    virtual void discard_expired(RecordHandle handle) = 0;

  private:
    std::size_t capacity_ = 0;
    RecordList records_;                     // live records, kept in strategy-defined order
    RecordList recycled_;                    // removed nodes, reused on creation
    Container<KeyType, RecordHandle> index_; // key -> handle
};

// -------- LRU --------
// Least-Recently-Used: every use moves the record to the front of the record
// list, so the back is always the least recently used record and thus the
// victim. The list *is* the whole strategy - no extra state at all.
template <typename KeyType, typename ValueType, template <typename, typename> class Container = HashMap>
class LRUStore : public Store<KeyType, ValueType, Container>
{
    using Base = Store<KeyType, ValueType, Container>;

  public:
    using Base::Base;
    using RecordHandle = typename Base::RecordHandle;

    std::optional<ValueType> get(const KeyType &key) override
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return std::nullopt;
        }
        this->promote(handle);
        return handle->get_value();
    }

    void set(const KeyType &key, std::optional<ValueType> value) override
    {
        const RecordHandle handle = resolve(key);
        if (handle != this->end())
        {
            if (!value.has_value()) // delete
            {
                this->remove(handle);
                return;
            }
            handle->set_value(std::move(*value));
            this->promote(handle); // a write is a use
            return;
        }

        if (!value.has_value())
        {
            return; // deleting a missing key is a no-op
        }

        if (this->is_full())
        {
            this->remove(this->back()); // the least recently used record
        }
        this->create(key, std::move(*value)); // lands at the front, i.e. newest
    }

    bool contains(const KeyType &key) override
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return false;
        }
        this->promote(handle);
        return true;
    }

    bool set_ttl(const KeyType &key, std::chrono::steady_clock::duration ttl) override
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return false;
        }
        handle->set_ttl(ttl);
        return true;
    }

    std::optional<std::chrono::milliseconds> get_ttl(const KeyType &key)
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return std::nullopt;
        }
        const auto ttl = handle->get_ttl();
        if (!ttl)
        {
            return std::nullopt;
        }
        return std::chrono::ceil<std::chrono::milliseconds>(*ttl);
    }

  private:
    void discard_expired(RecordHandle handle) override
    {
        this->remove(handle);
    }

    // Lazy expiry: a dead record is dropped here, so nothing above ever sees one.
    RecordHandle resolve(const KeyType &key)
    {
        const RecordHandle handle = this->find(key);
        if (handle != this->end() && !handle->is_alive())
        {
            this->remove(handle);
            return this->end();
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
template <typename KeyType, typename ValueType, template <typename, typename> class Container = HashMap>
class LFUStore : public Store<KeyType, ValueType, Container>
{
    using Base = Store<KeyType, ValueType, Container>;

  public:
    using RecordHandle = typename Base::RecordHandle;

    explicit LFUStore(std::size_t capacity = 0) : Base(capacity)
    {
    }

    std::optional<ValueType> get(const KeyType &key) override
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return std::nullopt;
        }
        bump(key);
        return handle->get_value();
    }

    void set(const KeyType &key, std::optional<ValueType> value) override
    {
        const RecordHandle handle = resolve(key);
        if (handle != this->end())
        {
            if (!value.has_value()) // delete
            {
                drop(handle);
                return;
            }
            handle->set_value(std::move(*value));
            bump(key); // a write is a use
            return;
        }

        if (!value.has_value())
        {
            return; // deleting a missing key is a no-op
        }

        if (this->is_full())
        {
            drop(victim());
        }
        admit(this->create(key, std::move(*value)));
    }

    bool contains(const KeyType &key) override
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return false;
        }
        bump(key);
        return true;
    }

    // Use count of a key, or 0 if it is not stored.
    std::size_t frequency(const KeyType &key) const
    {
        const auto it = entries_.find(key);
        return it == entries_.end() ? 0 : it->second.frequency;
    }

    bool set_ttl(const KeyType &key, std::chrono::steady_clock::duration ttl) override
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return false;
        }
        handle->set_ttl(ttl);
        return true;
    }

    std::optional<std::chrono::milliseconds> get_ttl(const KeyType &key)
    {
        const RecordHandle handle = resolve(key);
        if (handle == this->end())
        {
            return std::nullopt;
        }
        if (const auto expiry = handle->expiry_timepoint(); expiry.has_value())
        {
            return std::chrono::ceil<std::chrono::milliseconds>(*expiry - std::chrono::system_clock::now());
        }
        return std::nullopt;
    }

  private:
    void discard_expired(RecordHandle handle) override
    {
        drop(handle);
    }

    using HandleList = std::list<RecordHandle>;
    struct Entry
    {
        std::size_t frequency = 0;
        typename HandleList::iterator position; // where the handle sits in its bucket
    };

    // Lazy expiry, routed through Drop so the buckets stay exact.
    RecordHandle resolve(const KeyType &key)
    {
        const RecordHandle handle = this->find(key);
        if (handle != this->end() && !handle->is_alive())
        {
            drop(handle);
            return this->end();
        }
        return handle;
    }

    // remove a record from the store and from the frequency bookkeeping.
    void drop(RecordHandle handle)
    {
        if (handle == this->end())
        {
            return;
        }
        forget(handle->get_key());
        this->remove(handle);
    }

    // A new record enters with a use count of one.
    void admit(RecordHandle handle)
    {
        HandleList &bucket = buckets_[1];
        bucket.push_front(handle);
        entries_.insert_or_assign(handle->get_key(), Entry{.frequency = 1, .position = bucket.begin()});
    }

    // Move a handle from its bucket to the next one up.
    void bump(const KeyType &key)
    {
        const auto it = entries_.find(key);
        if (it == entries_.end())
        {
            return;
        }

        Entry &entry = it->second;
        const RecordHandle handle = *entry.position;
        erase(entry.frequency, entry.position);

        ++entry.frequency;
        HandleList &bucket = buckets_[entry.frequency];
        bucket.push_front(handle);
        entry.position = bucket.begin();
    }

    void forget(const KeyType &key)
    {
        const auto it = entries_.find(key);
        if (it == entries_.end())
        {
            return;
        }
        erase(it->second.frequency, it->second.position);
        entries_.erase(it);
    }

    void erase(std::size_t frequency, typename HandleList::iterator position)
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
    RecordHandle victim()
    {
        return buckets_.empty() ? this->end() : buckets_.begin()->second.back();
    }

    std::map<std::size_t, HandleList> buckets_;  // frequency -> handles, MRU at the front
    std::unordered_map<KeyType, Entry> entries_; // key -> {frequency, position in bucket}
};
} // namespace KV
