#pragma once
#if defined(__linux__)

#include "WriteHistory.hpp"

#include <Application/Commands.hpp>

#include <Foundation/Core/SocketAddress.hpp>
#include <Foundation/Core/BitmapMemory.hpp>
#include <Foundation/Core/RdmaAcceptor.hpp>
#include <Foundation/Core/RdmaConnector.hpp>
#include <Foundation/NBIO/ConditionVariable.hpp>
#include <Foundation/NBIO/RdmaAcceptChannel.hpp>
#include <Foundation/NBIO/RdmaConnectChannel.hpp>
#include <Foundation/NBIO/RdmaSessionService.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace KV
{
// RDMA replication, as a service the server plugs in: the service owns the
// wire and the replication port, the server owns the store and the clients.
// Handing a replica a snapshot is a pure read of the store, so a server can
// both follow a master and serve replicas of its own -- replication nests.
class ReplicationService
{
  public:
    // What the service asks of the server it plugs into.
    struct Host
    {
        // The file a snapshot is written to, which is also the file a replica is
        // given: the master streams it and the replica receives into it.
        std::function<std::filesystem::path()> snapshot_file;
        // Starts a background save. It must copy the store before it returns --
        // the caller starts the write log as soon as it does -- and the task it
        // hands back does the forking and the writing.
        std::function<Foundation::NBIO::Task<bool>()> snapshot;
        // Replaces the store with the RDB file it is given. False when the file
        // does not exist or does not validate against its own CRC-64.
        std::function<bool(const std::filesystem::path &)> restore;
        // Applies one command the master has applied, in the order it applied
        // them. Answers whether it could: a replica applies the writes its own
        // clients are refused, so this is the write path without the read-only
        // check, and it is the server's because the store is.
        std::function<bool(const Command &)> apply;
        // The link to the master came up, or went down.
        std::function<void(bool)> link_changed;
    };

    struct Options
    {
        // 0 leaves the master side of the service off.
        std::uint16_t listen_port{0};
        std::string listen_address{"0.0.0.0"};
        // The RDMA device, by name, that the link runs on. One name, one device,
        // one resource manager: replication does not yet fall back to TCP, so a
        // service that serves or follows needs it.
        std::string rdma_device{};
        // Set on a replica: the RDMA address of the master to synchronise from.
        std::optional<Foundation::Core::SocketAddress> master{};
        // The packet size: the size of one message and of one chunk in the
        // pools. Both ends have to agree on it, because it is the size the file
        // is cut at.
        std::size_t chunk_size{4096};
        // Chunks in each pool. A stream takes kSendChunks + kReceiveChunks of
        // them, so this is how many links the pools can carry at once.
        std::size_t chunk_count{128};
    };

    ReplicationService(Options options, Host host);
    ~ReplicationService() noexcept;

    ReplicationService(const ReplicationService &) = delete;
    ReplicationService &operator=(const ReplicationService &) = delete;
    ReplicationService(ReplicationService &&) = delete;
    ReplicationService &operator=(ReplicationService &&) = delete;

    bool is_master() const noexcept
    {
        return options_.listen_port != 0;
    }

    bool is_replica() const noexcept
    {
        return options_.master.has_value();
    }

    // Master side: accepts replicas and gives each one a full snapshot.
    Foundation::NBIO::Task<void> serve();

    // Replica side: synchronises once, then follows the master. It reconnects
    // on its own, so it only returns if the server stops it.
    Foundation::NBIO::Task<void> follow();

    // Records a command if the server is holding a log for a replica. The
    // server calls this for every write it applies, and it never waits: the log
    // hands the bytes to whichever stream asks for them.
    void record(const Command &command);

    // The writes this master has applied and is still holding for the replicas
    // that are following it.
    WriteHistory &history() noexcept
    {
        return history_;
    }

    const WriteHistory &history() const noexcept
    {
        return history_;
    }

  private:
    // Holds one replica's place in the write log for as long as that replica is
    // being served, however serve_replica() ends, including on a throw. The
    // cursor is taken at the instant the snapshot is captured, so a write lands
    // either in the image or in the log that follows it, never in both.
    struct Attachment
    {
        ReplicationService *service{nullptr};
        WriteHistory::Cursor *cursor{nullptr};

        Attachment() = default;
        explicit Attachment(ReplicationService &owner) : service(&owner), cursor(owner.attach())
        {
        }
        Attachment(const Attachment &) = delete;
        Attachment &operator=(const Attachment &) = delete;
        ~Attachment()
        {
            if (service != nullptr)
            {
                service->detach(cursor);
            }
        }
    };

    Foundation::NBIO::Task<void> serve_replica(std::shared_ptr<Foundation::NBIO::RdmaSessionService> session);
    // One full synchronization over a fresh link: the master's RDB file arrives,
    // is validated, and becomes the store. Answers the log offset the snapshot was
    // taken at -- where following on from it starts -- or nothing when that did
    // not happen.
    Foundation::NBIO::Task<std::optional<std::uint64_t>> full_sync(Foundation::NBIO::RdmaSessionService &session);
    // Keeps the link once the snapshot is done, applying the writes the master has
    // for this replica from `offset` onwards, one batch at a time.
    Foundation::NBIO::Task<void> follow_master(Foundation::NBIO::RdmaSessionService &session, std::uint64_t offset);

    // A cursor at the end of the log as it stands, which is where the snapshot
    // being served ends, and the recording that has to go with it. Nothing when
    // the log cannot be followed from here.
    WriteHistory::Cursor *attach() noexcept;
    void detach(WriteHistory::Cursor *cursor) noexcept;
    // Drops sessions whose coroutine has finished, so their chunks go back to
    // the pools and the next replica can be admitted.
    void prune_sessions();

    Options options_;
    Host host_;
    WriteHistory history_;
    // The writes that have landed, which is what a stream with nothing to send
    // waits on. Every replica's streamer waits on the same one and checks its own
    // place in the log when it wakes, so a wake-up it did not need costs a look
    // and nothing else.
    Foundation::NBIO::ConditionVariable writes_;

    // Declared before the sessions: every connection borrows the device, its
    // regions and its chunks, so they all have to be gone before it is. Shared,
    // because a connection keeps it alive for as long as it runs.
    std::shared_ptr<Foundation::Core::RdmaResourceManager> resources_;
    std::optional<Foundation::Core::RdmaAcceptor> acceptor_;

    struct ReplicaLink
    {
        explicit ReplicaLink(std::shared_ptr<Foundation::Core::RdmaResourceManager> manager) :
            resources(std::move(manager)), connector(*resources)
        {
        }

        // Held first, and held at all: the connection borrows this device.
        std::shared_ptr<Foundation::Core::RdmaResourceManager> resources;
        Foundation::Core::RdmaConnector connector;
        std::shared_ptr<Foundation::NBIO::RdmaSessionService> session;
    };
    std::unique_ptr<ReplicaLink> link_;

    std::vector<std::shared_ptr<Foundation::NBIO::RdmaSessionService>> sessions_;
};
} // namespace KV

#endif // defined(__linux__)
