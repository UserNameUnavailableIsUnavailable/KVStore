#pragma once

// Persistence: two independent mechanisms.
//
//  * Snapshot (SAVE) -- a FULL backup as a pure key/value dump (no TTL, no
//    command log). Restoring is a plain load, with no command replay, so it is
//    cheap. On Linux the write is done in a forked child: the parent keeps
//    serving while the child, over its copy-on-write snapshot of memory, writes
//    a consistent point-in-time image and atomically renames it into place.
//
//  * Append-only file (AOF) -- an INCREMENTAL backup: every successful mutation
//    is appended (RESP-encoded) to a file. Restoring replays those commands.
//
// Startup order: load the snapshot first (fast bulk restore), then replay the
// AOF on top (mutations since the snapshot).

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <sys/types.h>

#include "Command.hpp"

namespace KV
{
class Persistence
{
  public:
    // key/value visitor used to enumerate the live dataset for a snapshot.
    using KeyValueSink = std::function<void(std::string_view key, std::string_view value)>;
    // enumerates the live dataset (key,value) into the given sink.
    using ForEach = std::function<void(const KeyValueSink &)>;
    // applies a restored key/value pair to the store.
    using ApplyKeyValue = std::function<void(std::string key, std::string value)>;
    // applies a replayed command to the store.
    using ApplyCommand = std::function<void(const Request &)>;

    explicit Persistence(std::string directory);
    ~Persistence() noexcept;

    // ---- Snapshot (save) -------------------------------------------------
    // Fork a child that writes a pure-KV snapshot and atomically installs it.
    // Returns false if a save is already running or fork failed. Non-blocking:
    // the parent returns immediately; call ReapSave() to collect the child.
    bool save_snapshot(const ForEach &for_each);
    // Reap a finished save child. wait=false polls (WNOHANG); wait=true blocks.
    void reap_save(bool wait) noexcept;
    bool save_in_progress() const noexcept
    {
        return save_pid_ > 0;
    }
    // Load the latest snapshot (if any) via `apply`. Returns false if none.
    bool load_snapshot(const ApplyKeyValue &apply) const;

    // ---- Append-only file (AOF) -----------------------------------------
    bool enable_aof(); // open (create/append) the AOF file
    void disable_aof() noexcept;
    bool is_aof_enabled() const noexcept
    {
        return aof_fd_ >= 0;
    }
    // Append one already-encoded command to the AOF (best-effort, logged).
    void append_command(std::string_view encoded) noexcept;
    // Replay the AOF file (if any) by decoding commands and calling `apply`.
    void replay_aof(const ApplyCommand &apply) const;

  private:
    std::string snapshot_path() const;
    std::string aof_path() const;

    std::string directory_;
    int aof_fd_ = -1;
    ::pid_t save_pid_ = 0;
};
} // namespace KV
