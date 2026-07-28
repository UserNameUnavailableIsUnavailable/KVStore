#pragma once

namespace KV
{
// -------- Lifecycle policies --------
// A lifecycle governs whether a record is valid and reacts to accesses. It is
// deliberately container-agnostic: a record can be alive or dead on its own,
// with no container involved, so the lifecycle carries no notion of eviction.
// Eviction (choosing a victim, ordering records) belongs to the container that
// owns the record, not to the lifecycle. A lifecycle must expose:
//
//   bool IsAlive() const;  // whether the record is valid
//   void OnRead();         // fired when a valid record is read
//   void OnWrite();        // fired when a record is written a non-null value
//
// Storage fires these transitions on a record's behalf, so lifecycle management
// stays transparent: neither the eviction policy nor users of Storage ever call
// them. OnRead is where per-read accounting lives (no-op for LRU ordering,
// which the eviction policy handles; for LFU it would bump a frequency counter
// that the eviction policy later consults). OnWrite revives a record that had
// expired or been written away.

// Basic lifecycle: a record is born alive and stays alive once written. OnRead
// is a no-op. Pairs with LRU eviction.
class BasicLifecycle
{
public:
    bool IsAlive() const noexcept { return alive_; }
    void OnRead() noexcept {}
    void OnWrite() noexcept { alive_ = true; }

private:
    bool alive_ = true; // a record is born alive
};
} // namespace KV
