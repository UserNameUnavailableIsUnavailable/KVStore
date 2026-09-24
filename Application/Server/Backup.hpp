#pragma once

#include "Store.hpp"
#include <Foundation/NBIO/ConditionVariable.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Task.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sys/wait.h>
#endif

namespace KV
{
namespace detail
{
struct SnapshotEntry
{
    std::string key;
    std::string value;
    std::optional<std::chrono::milliseconds> ttl;
};

struct LoadedEntry
{
    std::string key;
    std::string value;
    std::optional<std::chrono::system_clock::time_point> expires_at;
};

bool WriteSnapshot(const std::filesystem::path &path, const std::vector<SnapshotEntry> &entries);

// Reads an RDB file and rejects it unless the CRC-64 it ends with matches the
// bytes before it, which is the check a replica makes before it replaces its
// store with what it was sent.
bool ReadSnapshot(const std::filesystem::path &path, std::vector<LoadedEntry> &entries);
} // namespace detail

class Backup
{
	public:
		explicit Backup(std::filesystem::path path = "dump.rdb") : path_(std::move(path))
		{
		}

        // Copies the live records of `store`. This is the point in time a
        // snapshot represents, and it runs on the calling thread, so a caller
        // that also keeps a log of the writes it receives can start recording
        // right after it and lose nothing in between.
        template <template <typename, typename> typename Map>
        static std::vector<detail::SnapshotEntry> capture(Store<std::string, std::string, Map> &store)
        {
            std::vector<detail::SnapshotEntry> entries;
            store.visit_live([&](const std::string &key, const std::string &value, const auto &ttl) {
                entries.push_back({.key = key, .value = value, .ttl = ttl});
            });
            return entries;
        }

        // Writes a snapshot of `entries` to the RDB path. The write happens in
        // a child process, so the event loop waits for it but is never the
        // process that forks.
        Foundation::NBIO::Task<bool> save(std::vector<detail::SnapshotEntry> entries) const;

        template <template <typename, typename> typename Map>
        Foundation::NBIO::Task<bool> save(Store<std::string, std::string, Map> &store) const
        {
            co_return co_await save(capture(store));
        }

        // Writes a snapshot of `entries` to the RDB path in this process, on the
        // calling thread: the store is serialised where the server runs, so
        // nothing else happens there until the image is written. That is the
        // difference between this and the save that forks, and the reason to ask
        // for one rather than the other.
        bool save_now(std::vector<detail::SnapshotEntry> entries) const;

        template <template <typename, typename> typename Map>
        bool save_now(Store<std::string, std::string, Map> &store) const
        {
            return save_now(capture(store));
        }

        // The file a snapshot is written to. It is also the file a replica is
        // handed, so it is what a replication service streams over the wire.
        const std::filesystem::path &path() const noexcept
        {
            return path_;
        }

        // Replaces `store` with the RDB file at `path`. False when the file does
        // not exist or does not validate against the CRC-64 it ends with.
        template <template <typename, typename> typename Map>
        bool load_from(const std::filesystem::path &path, Store<std::string, std::string, Map> &store) const
        {
            std::vector<detail::LoadedEntry> entries;
            if (!detail::ReadSnapshot(path, entries))
            {
                return false;
            }
            return apply(entries, store);
        }

        template <template <typename, typename> typename Map>
        bool load(Store<std::string, std::string, Map> &store) const
        {
            return load_from(path_, store);
        }

	private:
        template <template <typename, typename> typename Map>
        static bool apply(std::vector<detail::LoadedEntry> &entries, Store<std::string, std::string, Map> &store)
        {
            // An image says what exists, so a load is a replacement: whatever it
            // does not mention is gone. This is what makes a full sync on a
            // replica start from the master's keys and not from its own.
            std::vector<std::string> stale;
            store.visit_live([&](const std::string &key, const std::string &, const auto &) {
                stale.push_back(key);
            });
            for (const std::string &key : stale)
            {
                store.set(key, std::nullopt);
            }

            const auto now = std::chrono::system_clock::now();
            for (const auto &entry : entries)
            {
                if (entry.expires_at.has_value() && *entry.expires_at <= now)
                {
                    continue;
                }

                store.set(entry.key, entry.value);
                if (entry.expires_at.has_value())
                {
                    const auto remaining = std::chrono::duration_cast<std::chrono::steady_clock::duration>(*entry.expires_at - now);
                    if (remaining > std::chrono::steady_clock::duration::zero() && !store.set_ttl(entry.key, remaining))
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        // Runs `work` on a helper thread. Forking is the reason: a child of the
        // event loop thread would inherit the running backend.
        template <typename Work, typename Result = std::invoke_result_t<Work>>
        static Foundation::NBIO::Task<Result> offload(Work work)
        {
            Foundation::NBIO::ConditionVariable condition;
            std::optional<Result> result;
            std::atomic_bool done{false};
            std::atomic_bool start{false};
            std::thread worker([&work, &result, &condition, &done, &start] {
                while (!start.load(std::memory_order_acquire))
                {
                }
                try
                {
                    result.emplace(work());
                }
                catch (...)
                {
                    result.emplace(); // a default value is what a failure looks like here
                }
                done.store(true, std::memory_order_release);
                condition.notify_one();
            });
            co_await condition.wait([&start, &done] {
                start.store(true, std::memory_order_release);
                return done.load(std::memory_order_acquire);
            });
            worker.join();
            co_return std::move(*result);
        }

		std::filesystem::path path_;
};
} // namespace KV
