#pragma once

#include <utility>

#include "Common/Lifecycle.hpp"

namespace KV
{
// A record is a key, a value, and a lifecycle. The lifecycle is a composed
// member whose type is supplied by the owning Storage as a template parameter,
// so validity and per-access accounting can be customized without any virtual
// dispatch. See Lifecycle.hpp for the policy interface.
//
// Records are internal to the Storage layer. Lifecycle transitions (OnRead /
// OnWrite) are driven by the owning Storage; neither the eviction policy nor
// users of Storage ever touch them, which keeps lifecycle management
// transparent. A record is meaningful outside a container too: it is born
// alive and dies only when its lifecycle says so (e.g. a TTL expiring),
// independent of any eviction decision.
template <typename KeyType, typename ValueType, typename Lifecycle = BasicLifecycle>
class Record
{
public:
    template <typename K, typename V>
    Record(K&& key, V&& value) :
        key_(std::forward<K>(key)),
        value_(std::forward<V>(value))
    {
    }

    const KeyType& GetKey() const noexcept { return key_; }
    const ValueType& GetValue() const noexcept { return value_; }
    void SetValue(ValueType value) { value_ = std::move(value); }

    // Lifecycle state, driven by the owning Storage.
    bool IsAlive() const { return lifecycle_.IsAlive(); }
    void OnRead() { lifecycle_.OnRead(); }
    void OnWrite() { lifecycle_.OnWrite(); }

private:
    KeyType key_;
    ValueType value_;
    Lifecycle lifecycle_;
};
} // namespace KV
