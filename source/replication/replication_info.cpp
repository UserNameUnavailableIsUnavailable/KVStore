#include <replication/replication_info.hpp>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <limits>
#include <new>
#include <stdexcept>
#include <system_error>
#include <utility>

namespace replication {
namespace {
constexpr std::uint64_t kOwned = 1ULL << 63;
constexpr std::uint64_t kReserved = kOwned;

bool Release(ReplicationInfo& info, std::uint64_t ticket, bool owned) noexcept {
    if (!ticket || ticket >= kOwned) return false;
    auto expected = ticket | (owned ? kOwned : 0);
    auto& slot = info.handoffs[ticket % ReplicationInfo::kMaxReplicas];
    // Do not make the slot reusable until the decrement has completed.
    if (!slot.compare_exchange_strong(expected, kReserved, std::memory_order_acq_rel)) return false;
    info.replica_count.fetch_sub(1, std::memory_order_acq_rel);
    slot.store(0, std::memory_order_release);
    return true;
}
}

std::uint64_t ReplicationInfo::AttachReplica() {
    for (std::size_t attempt = 0; attempt < kMaxReplicas; ++attempt) {
        const auto ticket = next_ticket.fetch_add(1, std::memory_order_relaxed);
        if (!ticket || ticket >= kOwned) throw std::overflow_error("replication handoff tickets exhausted");
        auto& slot = handoffs[ticket % kMaxReplicas];
        std::uint64_t empty = 0;
        if (!slot.compare_exchange_strong(empty, kReserved, std::memory_order_acq_rel)) continue;
        replica_count.fetch_add(1, std::memory_order_acq_rel);
        slot.store(ticket, std::memory_order_release);
        return ticket;
    }
    throw std::runtime_error("replication handoff capacity exhausted");
}

bool ReplicationInfo::ClaimReplica(std::uint64_t ticket) noexcept {
    if (!ticket || ticket >= kOwned) return false;
    auto expected = ticket;
    return handoffs[ticket % kMaxReplicas].compare_exchange_strong(
        expected, ticket | kOwned, std::memory_order_acq_rel);
}

bool ReplicationInfo::CancelReplica(std::uint64_t ticket) noexcept { return Release(*this, ticket, false); }
bool ReplicationInfo::DetachReplica(std::uint64_t ticket) noexcept { return Release(*this, ticket, true); }

bool ReplicationInfo::IsPending(std::uint64_t ticket) const noexcept {
    return ticket && ticket < kOwned &&
        handoffs[ticket % kMaxReplicas].load(std::memory_order_acquire) == ticket;
}

struct SharedReplicationInfo::Mapping {
    int fd = -1;
    void* address = MAP_FAILED;
    ~Mapping() {
        if (address != MAP_FAILED) ::munmap(address, sizeof(ReplicationInfo));
        if (fd >= 0) ::close(fd);
    }
    void Map() {
        address = ::mmap(nullptr, sizeof(ReplicationInfo), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (address == MAP_FAILED) throw std::system_error(errno, std::generic_category(), "mmap ReplicationInfo");
    }
};

SharedReplicationInfo::SharedReplicationInfo(std::unique_ptr<Mapping> mapping) : mapping_(std::move(mapping)) {}
SharedReplicationInfo::~SharedReplicationInfo() = default;
SharedReplicationInfo::SharedReplicationInfo(SharedReplicationInfo&&) noexcept = default;
SharedReplicationInfo& SharedReplicationInfo::operator=(SharedReplicationInfo&&) noexcept = default;

SharedReplicationInfo SharedReplicationInfo::Create() {
    auto mapping = std::make_unique<Mapping>();
    mapping->fd = ::memfd_create("kvstore-replication-info", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (mapping->fd < 0) throw std::system_error(errno, std::generic_category(), "creating ReplicationInfo");
    if (::ftruncate(mapping->fd, sizeof(ReplicationInfo)) ||
        ::fcntl(mapping->fd, F_ADD_SEALS, F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL)) {
        throw std::system_error(errno, std::generic_category(), "sizing/sealing ReplicationInfo");
    }
    mapping->Map();
    new (mapping->address) ReplicationInfo;
    return SharedReplicationInfo(std::move(mapping));
}

SharedReplicationInfo SharedReplicationInfo::Map(int fd) {
    auto mapping = std::make_unique<Mapping>();
    mapping->fd = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if (mapping->fd < 0) throw std::system_error(errno, std::generic_category(), "duplicating ReplicationInfo");
    struct stat info{};
    if (::fstat(mapping->fd, &info)) throw std::system_error(errno, std::generic_category(), "stat ReplicationInfo");
    const auto seals = ::fcntl(mapping->fd, F_GET_SEALS);
    if (info.st_size != sizeof(ReplicationInfo) || seals < 0 ||
        (seals & (F_SEAL_GROW | F_SEAL_SHRINK)) != (F_SEAL_GROW | F_SEAL_SHRINK)) {
        throw std::runtime_error("invalid ReplicationInfo backing");
    }
    mapping->Map();
    const auto& header = *static_cast<ReplicationInfo*>(mapping->address);
    if (header.magic != ReplicationInfo::kMagic || header.version != ReplicationInfo::kVersion) {
        throw std::runtime_error("incompatible ReplicationInfo ABI");
    }
    return SharedReplicationInfo(std::move(mapping));
}

ReplicationInfo& SharedReplicationInfo::get() const noexcept {
    return *static_cast<ReplicationInfo*>(mapping_->address);
}
int SharedReplicationInfo::descriptor() const noexcept { return mapping_->fd; }
}  // namespace replication
