#include "Persistence.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "Protocol.hpp"

namespace KV
{
namespace
{
constexpr char kSnapshotMagic[4] = {'K', 'V', 'S', '1'};

// Blocking write of the whole buffer (handles short writes / EINTR).
bool WriteAll(int fd, const void *data, std::size_t size)
{
    const char *p = static_cast<const char *>(data);
    while (size > 0)
    {
        const ssize_t n = ::write(fd, p, size);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
        p += n;
        size -= static_cast<std::size_t>(n);
    }
    return true;
}

bool WriteField(int fd, std::string_view field)
{
    const std::uint32_t length = static_cast<std::uint32_t>(field.size());
    return WriteAll(fd, &length, sizeof(length)) && WriteAll(fd, field.data(), field.size());
}

// Reads exactly `size` bytes; returns false on EOF/short read.
bool ReadAll(int fd, void *data, std::size_t size)
{
    char *p = static_cast<char *>(data);
    while (size > 0)
    {
        const ssize_t n = ::read(fd, p, size);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
        if (n == 0)
        {
            return false; // unexpected EOF
        }
        p += n;
        size -= static_cast<std::size_t>(n);
    }
    return true;
}
} // namespace

Persistence::Persistence(std::string directory) : directory_(std::move(directory))
{
    std::error_code ec;
    std::filesystem::create_directories(directory_, ec);
}

Persistence::~Persistence() noexcept
{
    if (aof_fd_ >= 0)
    {
        ::close(aof_fd_);
    }
    if (save_pid_ > 0)
    {
        reap_save(true);
    }
}

std::string Persistence::snapshot_path() const
{
    return directory_ + "/dump.kv";
}
std::string Persistence::aof_path() const
{
    return directory_ + "/appendonly.aof";
}

// ---- Snapshot -----------------------------------------------------------

bool Persistence::save_snapshot(const ForEach &for_each)
{
    if (save_pid_ > 0)
    {
        reap_save(false); // maybe the previous one finished
        if (save_pid_ > 0)
        {
            return false; // still running: refuse to overlap
        }
    }

    const std::string final_path = snapshot_path();
    const std::string temp_path = final_path + ".tmp";

    const ::pid_t pid = ::fork();
    if (pid < 0)
    {
        return false;
    }
    if (pid == 0)
    {
        // Child: writes over a copy-on-write freeze of the dataset, then
        // installs it atomically. Uses _exit and touches no engine state.
        const int fd = ::open(temp_path.c_str(), O_WRONLY | O_CREAT | O_TrunC, 0644);
        if (fd < 0)
        {
            ::_exit(1);
        }
        bool ok = WriteAll(fd, kSnapshotMagic, sizeof(kSnapshotMagic));
        // Pure key/value, no TTL: restoring is a plain load, not a replay.
        for_each([&](std::string_view key, std::string_view value) {
            ok = ok && WriteField(fd, key) && WriteField(fd, value);
        });
        ::fsync(fd);
        ::close(fd);
        if (ok)
        {
            ok = (::rename(temp_path.c_str(), final_path.c_str()) == 0);
        }
        ::_exit(ok ? 0 : 1);
    }

    save_pid_ = pid; // parent: remember to reap
    return true;
}

void Persistence::reap_save(bool wait) noexcept
{
    if (save_pid_ <= 0)
    {
        return;
    }
    int status = 0;
    const ::pid_t r = ::waitpid(save_pid_, &status, wait ? 0 : WNOHANG);
    if (r == save_pid_ || r < 0)
    {
        save_pid_ = 0; // reaped, or gone
    }
}

bool Persistence::load_snapshot(const ApplyKeyValue &apply) const
{
    const int fd = ::open(snapshot_path().c_str(), O_RDONLY);
    if (fd < 0)
    {
        return false;
    }
    char magic[sizeof(kSnapshotMagic)];
    if (!ReadAll(fd, magic, sizeof(magic)) || std::memcmp(magic, kSnapshotMagic, sizeof(magic)) != 0)
    {
        ::close(fd);
        return false;
    }
    while (true)
    {
        std::uint32_t klen = 0;
        if (!ReadAll(fd, &klen, sizeof(klen)))
        {
            break; // clean EOF between records
        }
        std::string key(klen, '\0');
        std::uint32_t vlen = 0;
        if (!ReadAll(fd, key.data(), klen) || !ReadAll(fd, &vlen, sizeof(vlen)))
        {
            break;
        }
        std::string value(vlen, '\0');
        if (!ReadAll(fd, value.data(), vlen))
        {
            break;
        }
        apply(std::move(key), std::move(value));
    }
    ::close(fd);
    return true;
}

// ---- AOF ----------------------------------------------------------------

bool Persistence::enable_aof()
{
    if (aof_fd_ >= 0)
    {
        return true;
    }
    aof_fd_ = ::open(aof_path().c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    return aof_fd_ >= 0;
}

void Persistence::disable_aof() noexcept
{
    if (aof_fd_ >= 0)
    {
        ::close(aof_fd_);
        aof_fd_ = -1;
    }
}

void Persistence::append_command(std::string_view encoded) noexcept
{
    if (aof_fd_ < 0)
    {
        return;
    }
    // Best-effort durability; a stricter policy would fsync per write or on a
    // timer. Errors are swallowed here (a real build should surface them).
    (void)WriteAll(aof_fd_, encoded.data(), encoded.size());
}

void Persistence::replay_aof(const ApplyCommand &apply) const
{
    const int fd = ::open(aof_path().c_str(), O_RDONLY);
    if (fd < 0)
    {
        return;
    }
    // Slurp the whole file, then decode command by command.
    std::string data;
    char chunk[65536];
    while (true)
    {
        const ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            break;
        }
        if (n == 0)
        {
            break;
        }
        data.append(chunk, static_cast<std::size_t>(n));
    }
    ::close(fd);

    Protocol protocol;
    std::string_view input(data);
    while (!input.empty())
    {
        RequestDecode decoded = protocol.decode_request(input);
        if (decoded.status != DecodeStatus::kComplete)
        {
            break; // truncated / corrupt tail: stop replaying
        }
        apply(decoded.command);
        input.remove_prefix(decoded.consumed_bytes);
    }
}
} // namespace KV
