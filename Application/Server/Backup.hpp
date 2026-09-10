#pragma once

#include "Store.hpp"
#include <Foundation/Async/Async.hpp>
#include <Foundation/Async/Task.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <CRC.h>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__)
#include <sys/wait.h>
#endif

namespace KV
{
namespace backup_helper
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
bool ReadSnapshot(const std::filesystem::path &path, std::vector<LoadedEntry> &entries);
} // namespace backup_helper

class Backup
{
	public:
		explicit Backup(std::filesystem::path path = "dump.rdb") : path_(std::move(path))
		{
		}
        template <template <typename, typename> typename Map>
        Foundation::Async::Task<bool> save(Store<std::string, std::string, Map> &store) const
        {
            Foundation::Async::Condition condition;
            std::atomic_bool ok{false};
            std::atomic_bool done{false};
            std::thread worker([this, &store, &condition, &ok, &done] {
                ok.store(save_impl(store), std::memory_order_release);
                done.store(true, std::memory_order_release);
                condition.notify_one();
            });
            co_await condition.wait([&] {
                return done.load(std::memory_order_acquire);
            });
            worker.join();
            co_return ok.load(std::memory_order_acquire);
        }
        template <template <typename, typename> typename Map>
        bool load(Store<std::string, std::string, Map> &store) const
        {
            std::vector<backup_helper::LoadedEntry> entries;
            if (!backup_helper::ReadSnapshot(path_, entries))
            {
                return false;
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

	private:
        template <template <typename, typename> typename Map>
		bool save_impl(Store<std::string, std::string, Map> &store) const
        {
            std::vector<backup_helper::SnapshotEntry> entries;
            store.visit_live([&](const std::string &key, const std::string &value, const auto &ttl) {
                entries.push_back({.key = key, .value = value, .ttl = ttl});
            });
            const pid_t child = ::fork();
            if (child < 0)
            {
                return false;
            }
            if (child == 0)
            {
                bool ok = backup_helper::WriteSnapshot(path_, entries);
                _exit(ok ? EXIT_SUCCESS : EXIT_FAILURE);
            }

            int status = 0;
            if (::waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS)
            {
                return false;
            }
            return true;
        }

		std::filesystem::path path_;
};
} // namespace KV