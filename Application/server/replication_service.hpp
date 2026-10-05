#pragma once
#if defined(__linux__)

#include <Application/commands.hpp>
#include <nbio/async.hpp>
#include <nbio/net.hpp>
#include <nbio/notification/condition_variable.hpp>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace KV {
// Replication, as a service the server plugs in: the service owns the wire and
// the replication port, the server owns the store and the clients.
//
// A replica and a master talk RESP over an rdma delivery link -- the protocol a
// client speaks, with two commands of its own on top:
//
//   replica -> master   PSYNC ? -1
//   master  -> replica  +FULLRESYNC <replid> <offset>\r\n $<length>\r\n <RDB bytes>
//   replica -> master   PSYNC <replid> <offset>
//   master  -> replica  +CONTINUE <replid>\r\n <the writes, as they are applied>
//
// The opening request names no id, so it can only be answered with a snapshot.
// The second one is sent once that snapshot has been loaded, and it is what says
// the replica is ready for the stream. After it the master sends rather than
// being asked: every write it applies is encoded once and appended to each
// replica's buffer, and each replica's writer hands that buffer over as fast as
// the link allows.
//
// Nothing between the snapshot and the stream is lost, and nothing is sent
// twice, because the buffer is attached at the instant the snapshot is taken --
// the snapshot call copies the store before it returns, so a write lands either
// in the image or in the buffer that follows it.
//
// The link is an RdmaDeliverService rather than raw chunks. A sender that posts
// more messages than the peer has receives posted has its queue pair torn down
// under it for RNR, and only the delivery layer knows how far the peer has got.
class ReplicationService {
   public:
    // What the service asks of the server it plugs into.
    struct Host {
        // The file a snapshot is written to, which is also the file a replica is
        // given: the master streams it and the replica receives into it.
        std::function<std::filesystem::path()> snapshot_file;
        // Starts a background save. It must copy the store before it returns --
        // the buffer that follows the snapshot is attached as soon as it does --
        // and the task it hands back does the forking and the writing.
        std::function<nbio::async::Task<bool>()> snapshot;
        // Replaces the store with the RDB file it is given. False when the file
        // does not exist or does not validate against its own CRC-64.
        std::function<bool(const std::filesystem::path&)> restore;
        // Applies one command the master has applied, in the order it applied
        // them. Answers whether it could: a replica applies the writes its own
        // clients are refused, so this is the write path without the read-only
        // check, and it is the server's because the store is.
        std::function<bool(const Command&)> apply;
        // The link to the master came up, or went down.
        std::function<void(bool)> link_changed;
    };

    struct Options {
        // 0 leaves the master side of the service off.
        std::uint16_t listen_port{0};
        std::string listen_address{"0.0.0.0"};
        // The rdma device, by name, that the link runs on. One name, one device,
        // one resource manager: replication does not fall back to TCP, so a
        // service that serves or follows needs it.
        std::string rdma_device{};
    };

    ReplicationService(Options options, Host host);
    ~ReplicationService() noexcept;

    ReplicationService(const ReplicationService&) = delete;
    ReplicationService& operator=(const ReplicationService&) = delete;
    ReplicationService(ReplicationService&&) = delete;
    ReplicationService& operator=(ReplicationService&&) = delete;

    bool is_master() const noexcept { return options_.listen_port != 0; }

    // True from the moment SLAVEOF is accepted rather than from the moment the
    // link comes up: an instance told to follow a master is a replica while it
    // is still connecting, and what its clients may write does not depend on the
    // state of a socket.
    bool is_replica() const noexcept { return following_.load(std::memory_order_acquire); }

    // This master's replication id: an integer, taken when the service is built.
    // A replica quotes it back, so a master that does not recognise it is a
    // master that restarted.
    std::uint64_t replid() const noexcept { return replid_; }

    // Master side: accepts replicas, gives each one a snapshot, and then sends
    // it the writes this master applies.
    nbio::async::Task<void> serve();

    // Makes this instance a replica of `master`, and keeps it one: the link is
    // re-established for as long as the service lives. False when this instance
    // already follows a master, which is the one thing SLAVEOF cannot do twice.
    bool slave_of(const nbio::net::Address& master);

    // Records a write for the replicas being served. The server calls this for
    // every write it applies, and it never waits: the bytes are appended to each
    // replica's buffer, and whoever is draining that buffer finds them there.
    void record(const Command& command);

   private:
    // One replica being served: its link, and what is waiting to go out on it.
    struct Replica {
        Replica(std::shared_ptr<nbio::net::RdmaSessionService> session,
                std::shared_ptr<nbio::net::RdmaDeliverService> link)
            : session(std::move(session)), link(std::move(link)) {}

        std::shared_ptr<nbio::net::RdmaSessionService> session;
        std::shared_ptr<nbio::net::RdmaDeliverService> link;
        // Encoded writes this replica has not been sent. A write is appended
        // whether or not the replica may be sent one yet, so what was applied
        // between the snapshot and the second PSYNC is held here rather than
        // missed -- and a link that is momentarily full buffers here rather than
        // dropping anything.
        std::string pending;
        // Set once the replica has loaded the snapshot and asked for the stream.
        // Until then this buffer only fills.
        bool streaming{false};
        bool ended{false};
        nbio::notification::ConditionVariable writable;
    };

    // Holds one replica in the record() path for as long as it is being served,
    // however serve_replica() ends, including on a throw. begin() is what puts it
    // there, and it is called at the instant the snapshot is captured -- so the
    // attachment outlives the snapshot phase, which is the whole point: the writes
    // applied while the snapshot is on the wire have to land somewhere.
    struct Attachment {
        ReplicationService* service{nullptr};
        std::shared_ptr<Replica> replica;

        Attachment() = default;
        Attachment(const Attachment&) = delete;
        Attachment& operator=(const Attachment&) = delete;
        ~Attachment() {
            if (service != nullptr) {
                service->detach(replica);
            }
        }

        void begin(ReplicationService& owner, std::shared_ptr<Replica> served) {
            service = &owner;
            replica = std::move(served);
            service->replicas_.push_back(replica);
        }
    };

    nbio::async::Task<void> serve_replica(std::shared_ptr<nbio::net::RdmaSessionService> session);
    // Hands one replica's buffer to the link, waiting for the write that is
    // going to fill it. Runs for as long as the replica does.
    nbio::async::Task<void> pump(std::shared_ptr<Replica> replica);
    // Answers the opening PSYNC with a snapshot, then waits for the replica to
    // say it has loaded it. False when the link did not survive that. The
    // attachment is begun here, at the snapshot, and is left holding the replica
    // for the caller.
    nbio::async::Task<bool> send_snapshot(std::shared_ptr<Replica> replica, Attachment& attachment);

    // Replica side, for as long as this instance is one.
    nbio::async::Task<void> follow_forever(nbio::net::Address master);
    // One connection's worth: connect, synchronise, and keep up until the link
    // ends. False when that ended before the stream did.
    nbio::async::Task<bool> sync_once(nbio::net::Address master);

    void detach(const std::shared_ptr<Replica>& replica) noexcept;
    // Drops the replicas whose coroutine has finished, so their chunks go back
    // to the pools and the next replica can be admitted.
    void prune();

    // A layout for a delivery link: the receive chunks of the manager this
    // service borrows, and as many packets in flight as the connection posts
    // receives for -- which is the only number the peer may fill.
    nbio::net::RdmaDeliverService::Layout link_layout() const;

    Options options_;
    Host host_;
    std::uint64_t replid_{0};
    // The offset of the byte after the last write this master applied: the
    // position a snapshot is taken at and the position a replica is caught up
    // from, going on across links coming and going.
    std::uint64_t offset_{0};

    // Declared before the sessions: every connection borrows the device, its
    // regions and its chunks, so they all have to be gone before it is. Shared,
    // because a connection keeps it alive for as long as it runs.
    std::shared_ptr<nbio::net::RdmaResourceManager> resources_;
    std::optional<nbio::net::RdmaAcceptService> acceptor_;

    // The replicas being served. This vector is the record() path's list, so a
    // replica is held here for as long as its buffer has to be filled.
    std::vector<std::shared_ptr<Replica>> replicas_;

    // The replica half. `following_` is what SLAVEOF sets and what the read-only
    // check reads; the link itself is rebuilt on every reconnect, because a
    // successful connect hands its communication id to the session and cannot be
    // used twice.
    std::atomic_bool following_{false};

    struct ReplicaLink {
        explicit ReplicaLink(std::shared_ptr<nbio::net::RdmaResourceManager> manager)
            : resources(std::move(manager)), connector(*resources) {}

        // Held first, and held at all: the connection borrows this device. The
        // link is declared last so that it is destroyed before what it borrows.
        std::shared_ptr<nbio::net::RdmaResourceManager> resources;
        nbio::net::RdmaConnectService connector;
        std::shared_ptr<nbio::net::RdmaDeliverService> link;
    };
    std::unique_ptr<ReplicaLink> link_;
};
}  // namespace KV

#endif  // defined(__linux__)
