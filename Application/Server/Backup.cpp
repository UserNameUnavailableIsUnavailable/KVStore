#include "Backup.hpp"

#include <CRC.h>

#include <Foundation/Byte.hpp>
#include <Foundation/FileView.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#if defined(__linux__)
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace KV
{
namespace backup_helper
{
constexpr std::uint8_t kAux = 0xFA;
constexpr std::uint8_t kResizeDb = 0xFB;
constexpr std::uint8_t kExpireTimeMs = 0xFC;
constexpr std::uint8_t kSelectDb = 0xFE;
constexpr std::uint8_t kEof = 0xFF;
constexpr std::uint8_t kString = 0x00;

void Write(std::ofstream &file, const void *data, std::size_t size)
{
    file.write(static_cast<const char *>(data), static_cast<std::streamsize>(size));
}

void WriteByte(std::ofstream &file, std::uint8_t value)
{
    Write(file, &value, sizeof(value));
}

void WriteLength(std::ofstream &file, std::uint64_t value)
{
    if (value < (1U << 6U))
    {
        WriteByte(file, static_cast<std::uint8_t>(value));
    }
    else if (value < (1U << 14U))
    {
        WriteByte(file, static_cast<std::uint8_t>(0x40U | (value >> 8U)));
        WriteByte(file, static_cast<std::uint8_t>(value));
    }
    else if (value <= std::numeric_limits<std::uint32_t>::max())
    {
        WriteByte(file, 0x80U);
        const std::uint32_t big_endian = Foundation::to_big_endian(static_cast<std::uint32_t>(value));
        Write(file, &big_endian, sizeof(big_endian));
    }
    else
    {
        WriteByte(file, 0x81U);
        const std::uint64_t big_endian = Foundation::to_big_endian(value);
        Write(file, &big_endian, sizeof(big_endian));
    }
}

void WriteString(std::ofstream &file, std::string_view value)
{
    WriteLength(file, value.size());
    Write(file, value.data(), value.size());
}

void WriteAux(std::ofstream &file, std::string_view key, std::string_view value)
{
    WriteByte(file, kAux);
    WriteString(file, key);
    WriteString(file, value);
}

std::uint64_t Checksum(const std::filesystem::path &path)
{
    std::ifstream file(path, std::ios::binary);
    const std::string bytes{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    constexpr CRC::Parameters<std::uint64_t, 64> kRedisCrc64{
        0xAD93D23594C935A9ULL, 0x0000000000000000ULL, 0x0000000000000000ULL, true, true};
    return CRC::Calculate(bytes.data(), bytes.size(), kRedisCrc64);
}

bool ReadExact(std::span<const std::uint8_t> bytes, std::size_t &offset, void *data, std::size_t size)
{
    if (offset + size > bytes.size())
    {
        return false;
    }
    std::memcpy(data, bytes.data() + offset, size);
    offset += size;
    return true;
}

bool ReadByte(std::span<const std::uint8_t> bytes, std::size_t &offset, std::uint8_t &value)
{
    return ReadExact(bytes, offset, &value, sizeof(value));
}

bool ReadLength(std::span<const std::uint8_t> bytes, std::size_t &offset, std::uint64_t &value)
{
    std::uint8_t header = 0;
    if (!ReadByte(bytes, offset, header))
    {
        return false;
    }

    if ((header & 0xC0U) == 0x00U)
    {
        value = header & 0x3FU;
        return true;
    }
    if ((header & 0xC0U) == 0x40U)
    {
        std::uint8_t low = 0;
        if (!ReadByte(bytes, offset, low))
        {
            return false;
        }
        value = (static_cast<std::uint64_t>(header & 0x3FU) << 8U) | low;
        return true;
    }
    if (header == 0x80U)
    {
        std::uint32_t big_endian = 0;
        if (!ReadExact(bytes, offset, &big_endian, sizeof(big_endian)))
        {
            return false;
        }
        value = Foundation::from_big_endian(big_endian);
        return true;
    }
    if (header == 0x81U)
    {
        std::uint64_t big_endian = 0;
        if (!ReadExact(bytes, offset, &big_endian, sizeof(big_endian)))
        {
            return false;
        }
        value = Foundation::from_big_endian(big_endian);
        return true;
    }
    return false;
}

bool ReadString(std::span<const std::uint8_t> bytes, std::size_t &offset, std::string &value)
{
    std::uint64_t length = 0;
    if (!ReadLength(bytes, offset, length))
    {
        return false;
    }
    if (offset + length > bytes.size())
    {
        return false;
    }
    value.assign(reinterpret_cast<const char *>(bytes.data() + offset), static_cast<std::size_t>(length));
    offset += static_cast<std::size_t>(length);
    return true;
}

bool ValidateChecksum(std::span<const std::uint8_t> bytes)
{
    if (bytes.size() < sizeof(std::uint64_t))
    {
        return false;
    }

    std::uint64_t stored_checksum = 0;
    std::memcpy(&stored_checksum, bytes.data() + (bytes.size() - sizeof(stored_checksum)), sizeof(stored_checksum));
    constexpr CRC::Parameters<std::uint64_t, 64> kRedisCrc64{
        0xAD93D23594C935A9ULL, 0x0000000000000000ULL, 0x0000000000000000ULL, true, true};
    const auto computed = CRC::Calculate(bytes.data(), bytes.size() - sizeof(stored_checksum), kRedisCrc64);
    return computed == stored_checksum;
}

bool ReadSnapshotImpl(std::span<const std::uint8_t> bytes, std::vector<LoadedEntry> &entries)
{
    entries.clear();
    if (bytes.empty())
    {
        return true;
    }

    if (bytes.size() < 9 + sizeof(std::uint64_t))
    {
        return false;
    }

    if (!ValidateChecksum(bytes))
    {
        return false;
    }

    std::size_t offset = 0;
    constexpr char kHeader[] = "REDIS0006";
    if (bytes.size() < sizeof(kHeader) - 1 + sizeof(std::uint64_t) ||
        std::memcmp(bytes.data(), kHeader, sizeof(kHeader) - 1) != 0)
    {
        return false;
    }
    offset += sizeof(kHeader) - 1;

    while (offset < bytes.size() - sizeof(std::uint64_t))
    {
        std::uint8_t op = 0;
        if (!ReadByte(bytes, offset, op))
        {
            return false;
        }

        if (op == kAux)
        {
            std::string key;
            std::string value;
            if (!ReadString(bytes, offset, key) || !ReadString(bytes, offset, value))
            {
                return false;
            }
            continue;
        }
        if (op == kSelectDb)
        {
            std::uint64_t db = 0;
            if (!ReadLength(bytes, offset, db))
            {
                return false;
            }
            continue;
        }
        if (op == kResizeDb)
        {
            std::uint64_t keys = 0;
            std::uint64_t expires = 0;
            if (!ReadLength(bytes, offset, keys) || !ReadLength(bytes, offset, expires))
            {
                return false;
            }
            continue;
        }
        if (op == kEof)
        {
            break;
        }

        std::optional<std::chrono::system_clock::time_point> expires_at;
        if (op == kExpireTimeMs)
        {
            std::uint64_t deadline_ms = 0;
            if (!ReadExact(bytes, offset, &deadline_ms, sizeof(deadline_ms)))
            {
                return false;
            }
            expires_at = std::chrono::system_clock::time_point{std::chrono::milliseconds{deadline_ms}};
            if (!ReadByte(bytes, offset, op))
            {
                return false;
            }
        }

        if (op != kString)
        {
            return false;
        }

        LoadedEntry entry;
        if (!ReadString(bytes, offset, entry.key) || !ReadString(bytes, offset, entry.value))
        {
            return false;
        }
        entry.expires_at = std::move(expires_at);
        entries.push_back(std::move(entry));
    }

    return true;
}

bool WriteSnapshot(const std::filesystem::path &path, const std::vector<SnapshotEntry> &entries)
{
    const std::filesystem::path temporary = path.string() + ".tmp";
    std::ofstream file(temporary, std::ios::binary | std::ios::trunc);
    if (!file)
    {
        return false;
    }

    Write(file, "REDIS0006", 9);
    WriteAux(file, "redis-ver", "KVStore");
    WriteAux(file, "redis-bits", std::to_string(sizeof(void *) * 8));
    WriteAux(file, "ctime", std::to_string(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now())));
    WriteAux(file, "used-mem", "0");
    WriteByte(file, kSelectDb);
    WriteLength(file, 0);

    WriteByte(file, kResizeDb);
    WriteLength(file, entries.size());
    WriteLength(file, std::count_if(entries.begin(), entries.end(), [](const SnapshotEntry &entry) {
        return entry.ttl.has_value();
    }));

    for (const SnapshotEntry &entry : entries)
    {
        if (entry.ttl)
        {
            WriteByte(file, kExpireTimeMs);
            const auto deadline = std::chrono::system_clock::now() + *entry.ttl;
            const std::uint64_t milliseconds = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline.time_since_epoch()).count());
            Write(file, &milliseconds, sizeof(milliseconds));
        }
        WriteByte(file, kString);
        WriteString(file, entry.key);
        WriteString(file, entry.value);
    }
    WriteByte(file, kEof);
    file.flush();
    file.close();
    if (!file)
    {
        return false;
    }

    const std::uint64_t checksum = Checksum(temporary);
    std::ofstream checksum_file(temporary, std::ios::binary | std::ios::app);
    Write(checksum_file, &checksum, sizeof(checksum));
    checksum_file.close();
    if (!checksum_file)
    {
        return false;
    }
    std::filesystem::rename(temporary, path);
    return true;
}

bool ReadSnapshot(const std::filesystem::path &path, std::vector<LoadedEntry> &entries)
{
    if (!std::filesystem::exists(path))
    {
        entries.clear();
        return true;
    }
    Foundation::File file(path, Foundation::FileMode::kRead);
    Foundation::FileView view(file);
    if (view.size() == 0)
    {
        entries.clear();
        return true;
    }

    const auto *bytes_begin = static_cast<const std::uint8_t *>(view.data());
    return ReadSnapshotImpl(std::span<const std::uint8_t>(bytes_begin, view.size()), entries);
}
} // namespace backup_helper
} // namespace KV