#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace replication {
// Linux, same-ABI shared metadata, not a command buffer. Slots arbitrate the
// decrement owner across failed RDMA -> TCP handoffs; tokens prevent stale reuse.
struct alignas(64) ReplicationInfo {
    static constexpr std::uint64_t kMagic = 0x4b565245504c4931ULL;
    static constexpr std::uint64_t kVersion = 1;
    static constexpr std::size_t kMaxReplicas = 128;

    const std::uint64_t magic = kMagic;
    const std::uint64_t version = kVersion;
    alignas(64) std::atomic<std::uint64_t> replica_count{0};
    std::atomic<std::uint64_t> daemon_ready{0};
    std::atomic<std::uint64_t> next_ticket{1};
    std::array<std::atomic<std::uint64_t>, kMaxReplicas> handoffs{};

    // Master: increment before snapshot; cancel only while the handoff is pending.
    std::uint64_t AttachReplica();
    bool CancelReplica(std::uint64_t ticket) noexcept;
    // Daemon: inherit without incrementing; detach decrements only owned tickets.
    bool ClaimReplica(std::uint64_t ticket) noexcept;
    bool DetachReplica(std::uint64_t ticket) noexcept;
    bool IsPending(std::uint64_t ticket) const noexcept;
};

static_assert(std::atomic<std::uint64_t>::is_always_lock_free);

class SharedReplicationInfo {
public:
    static SharedReplicationInfo Create();
    static SharedReplicationInfo Map(int fd);
    ~SharedReplicationInfo();
    SharedReplicationInfo(SharedReplicationInfo&&) noexcept;
    SharedReplicationInfo& operator=(SharedReplicationInfo&&) noexcept;
    SharedReplicationInfo(const SharedReplicationInfo&) = delete;
    SharedReplicationInfo& operator=(const SharedReplicationInfo&) = delete;
    ReplicationInfo& get() const noexcept;
    int descriptor() const noexcept;
private:
    struct Mapping;
    explicit SharedReplicationInfo(std::unique_ptr<Mapping> mapping);
    std::unique_ptr<Mapping> mapping_;
};
}  // namespace replication
