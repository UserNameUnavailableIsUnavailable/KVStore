#pragma once

#include <cstddef>
#include <iterator>
#include <list>
#include <memory_resource>
#include <utility>

namespace KV
{
// -------- Eviction-policy concept --------
// An eviction policy is the container that owns the records, keeping them in a
// policy-defined order and handing out stable ElementHandles that the key
// index stores. It is a compile-time policy (no virtual dispatch), so its
// per-access hooks inline on the hot path.
//
// A policy must expose:
//   using ElementHandle;                            // stable, copyable handle
//   template <class... Args> ElementHandle Insert(Args&&...); // add, return handle
//   void          Touch(ElementHandle);             // called on an access hit
//   void          Erase(ElementHandle);             // remove a record
//   ElementHandle Victim();                         // eviction candidate
//   Record&       At(ElementHandle) noexcept;       // dereference a handle
//   std::size_t   Size() const noexcept;
//
// The ElementHandle type is defined by each policy, so different structures
// (LRU list, LFU frequency buckets, ...) can satisfy the same interface.

// Least-Recently-Used: an intrusive-order doubly linked list whose front is the
// most-recently-used record and whose back is the next eviction victim.
template <typename Record>
class LruPolicy
{
    using Container = std::pmr::list<Record>;

public:
    using ElementHandle = typename Container::iterator;

    explicit LruPolicy(std::pmr::memory_resource* resource) :
        records_(resource)
    {
    }

    template <typename... Args>
    ElementHandle Insert(Args&&... args)
    {
        records_.emplace_front(std::forward<Args>(args)...);
        return records_.begin();
    }

    // Splicing does not invalidate iterators, so ElementHandles stored in the
    // key index stay valid across accesses.
    void Touch(ElementHandle handle) { records_.splice(records_.begin(), records_, handle); }

    void Erase(ElementHandle handle) { records_.erase(handle); }

    ElementHandle Victim() { return std::prev(records_.end()); }

    Record& At(ElementHandle handle) noexcept { return *handle; }

    std::size_t Size() const noexcept { return records_.size(); }

private:
    Container records_;
};
} // namespace KV
