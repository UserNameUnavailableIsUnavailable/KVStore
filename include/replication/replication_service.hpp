#pragma once
#if defined(__linux__)

#include <backup/backup.hpp>
#include <commands.hpp>
#include <nbio/async.hpp>
#include <nbio/fs/file_view.hpp>
#include <nbio/net.hpp>
#include <nbio/notification/condition_variable.hpp>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <replication/command_backlog.hpp>
#include <replication/stream_protocol.hpp>

namespace KV {
// Replication is a push from the master, not a draw from the follower.
//
// The master keeps one bounded backlog of the writes it has applied, each tagged
// with its end offset (the logical write counter). A replica connects and sends
// `REPLICA` with no arguments; the master answers `+CONTINUE <epoch> <head>` and
// from then on copies every command it executes to that connection, in order, as
// `@<epoch> <end_offset> <length>\r\n<payload>`. The follower never chooses an
// offset: it is pushed the stream from the moment it connected. Nothing
// per-follower lives on the master except the connection and its cursor.
//
// RDMA carries only the full snapshot. A SYNC answers
// `+FULLSYNC <bytes> <rdma_ip>:<rdma_port> <offset> <epoch>`, where <offset> is
// the write counter when the snapshot was taken: a follower that has buffered the
// pushed stream (from its connect offset) discards records up to it and continues
// at <offset>+1.
//
// A replica may take the pushed stream from the master directly, or from a broker
// that mirrors the master and fans out to replicas. Either way it buffers while it
// fetches the snapshot from the master, then replays the buffer from the snapshot
// offset.
class ReplicationService {
   public:
    struct Host {
        std::function<std::filesystem::path()> snapshot_file;
        // Forks synchronously; Wait() only observes the child's completion.
        std::function<ForkSnapshot()> snapshot;
        std::function<std::uint64_t()> offset;
        // Replaces the store with the RDB file it is given.
        std::function<bool(const std::filesystem::path&)> restore;
        // Applies one command the master has applied, in the order it applied it.
        std::function<bool(const Command&)> apply;
        // The link to the master came up, or went down.
        std::function<void(bool)> link_changed;
    };

    struct Options {
        // The rdma device's address; non-empty makes this instance a master that
        // serves dumps.
        std::string listen_address{};
        std::string rdma_device{};
        // Optional broker "ip:port" a replica subscribes to for the pushed stream
        // (empty: take it from the master directly).
        std::string broker_endpoint{};
        std::size_t backlog_bytes{64U * 1024U * 1024U};
        std::uint64_t epoch = 0;
    };

    ReplicationService(Options options, Host host);
    ~ReplicationService() noexcept;

    ReplicationService(const ReplicationService&) = delete;
    ReplicationService& operator=(const ReplicationService&) = delete;
    ReplicationService(ReplicationService&&) = delete;
    ReplicationService& operator=(ReplicationService&&) = delete;

    bool is_master() const noexcept { return !options_.listen_address.empty(); }

    // True from the moment SLAVEOF is accepted: an instance told to follow a
    // master is a replica while it is still connecting.
    bool is_replica() const noexcept { return following_.load(std::memory_order_acquire); }

    // This master's replication id and the last offset it has applied.
    std::uint64_t replid() const noexcept { return replid_; }
    std::uint64_t head_offset() const noexcept { return backlog_.head(); }

    // What the replica is told about the dump it is about to receive.
    struct SyncOffer {
        std::uint64_t bytes{0};
        std::string rdma_ip;
        std::uint16_t rdma_port{0};
        std::uint64_t offset{0};
        std::uint64_t epoch{0};
    };

    // Master side, driven by a SYNC over TCP: forks the snapshot, mmaps it, and
    // prepares the RDMA listener that will carry it. Serialized, because the
    // dump file is one path and only one child may write it at a time. Also
    // resets the backlog to the snapshot offset.
    nbio::async::Task<nbio::utility::expected<SyncOffer, std::error_code>> OfferDump();

    // Master side: accepts one RDMA connection and sends the offered dump over it.
    nbio::async::Task<void> ServeDump();

    // Releases an offered dump without serving it.
    void AbortDump() noexcept;

    // Starts the master's standing tasks (none today; kept for symmetry).
    void Start();

    // Master side: registers `session` and copies every command the master
    // executes to it, from the head at registration onward. Terminal for `session`.
    nbio::async::Task<void> follow(nbio::net::TcpSessionService& session);

    // Makes this instance a replica of `master`, whose TCP client address it is.
    bool slave_of(const nbio::net::Address& master);

    // Publishes one applied write to the backlog. Called on the write path; never
    // waits.
    void record(std::string_view payload);

   private:
    class PushFeed;

    nbio::async::Task<void> follow_forever(nbio::net::Address master);
    // One SYNC: dump over RDMA, restored locally. Returns the snapshot offset and
    // records the master epoch.
    nbio::async::Task<std::optional<std::uint64_t>> sync_once(nbio::net::Address master);
    void release_snapshot() noexcept;

    bool apply_payload(std::string_view payload);

    Options options_;
    Host host_;
    std::uint64_t replid_{0};
    bool snapshot_busy_ = false;
    nbio::notification::ConditionVariable snapshot_ready_;
    replication::CommandBacklog backlog_;
    nbio::notification::ConditionVariable backlog_changed_;

    struct ActiveDump {
        std::unique_ptr<nbio::fs::FileView> mapping;
        std::unique_ptr<char, decltype(&std::free)> receive_arena{nullptr, &std::free};
        std::unique_ptr<nbio::net::RdmaResource> resource;
        std::unique_ptr<nbio::net::RdmaListenService> listener;
        std::uint64_t bytes{0};
    };
    std::optional<ActiveDump> active_dump_;

    std::unique_ptr<PushFeed> feed_;
    std::uint64_t master_epoch_{0};
    std::atomic_bool following_{false};
};
}  // namespace KV

#endif  // defined(__linux__)
