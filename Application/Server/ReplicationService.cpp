#include "ReplicationService.hpp"

#if defined(__linux__)

#include "RdmaTransfer.hpp"

#include <Application/Commands.hpp>
#include <Application/RESP/RESP.hpp>
#include <Foundation/Async/Async.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/NBIO/Engine.hpp>
#include <Foundation/NBIO/NBIO.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>
#include <utility>

namespace KV
{
namespace
{
namespace Core = Foundation::Core;
namespace NBIO = Foundation::NBIO;

// Waits for the link to end. Nothing follows a snapshot yet, so this is where
// the master's side of a live link spends its time.
NBIO::Task<void> ParkUntilPeerCloses(NBIO::RdmaSession &session)
{
    while (true)
    {
        auto received = co_await session.receive();
        if (!received || !*received)
        {
            // The link is gone, or there was nothing left to take. Either way
            // there is nothing more to wait for.
            break;
        }
        if (const auto released = session.release(**received); !released) [[unlikely]]
        {
            spdlog::warn("replication: handing a message back failed: {}", released.error());
            break;
        }
    }
}

std::string Endpoint(const Core::SocketAddress &address)
{
    return address.ip() + ":" + std::to_string(address.port());
}
} // namespace

ReplicationService::ReplicationService(Options options, Host host) :
    options_(std::move(options)), host_(std::move(host))
{
    // The transfer opens with the receiver's chunk size and window and answers
    // with the sender's decision, so the agreed chunk size has to be able to
    // carry those messages and the sender's plan.
    if (options_.chunk_size < kPlanMessageBytes)
    {
        throw std::invalid_argument("replication: the packet size cannot hold the count the transfer announces");
    }
    if (options_.chunk_count < 2 * Core::RdmaConnector::kSendChunks)
    {
        throw std::invalid_argument("replication: the pools are too small to serve one connection");
    }
    // Replication runs on RDMA, and only on RDMA: there is no TCP path yet, so a
    // service that serves or follows without a device named would come up unable
    // to do either. An instance that does neither needs nothing.
    if ((options_.listen_port != 0 || options_.master.has_value()) && options_.rdma_device.empty())
    {
        throw std::invalid_argument("replication: replication needs an RDMA device; pass --rdma-device or "
                                    "'config rdma_device <name>'");
    }
    // A wildcard address is accepted by rdma_bind_addr and leaves the id with no
    // device, so a listener asked to serve replicas from one has nothing to serve
    // them from -- and everything that follows the bind needs a device. Refuse it
    // while the reason is still known.
    if (options_.listen_port != 0 &&
        (options_.listen_address.empty() || options_.listen_address == "0.0.0.0" || options_.listen_address == "::"))
    {
        throw std::invalid_argument("replication: serving replicas needs the address of an RDMA device, not '" +
                                    options_.listen_address + "'");
    }
    // An address goes in one option and the port in another, and `ip:port` in the
    // address is the way that gets written by mistake. Say which option takes
    // what, whether or not a listener is being started: a setting that is wrong is
    // worth knowing about even when nothing is reading it yet.
    if (options_.listen_address.find(':') != std::string::npos)
    {
        throw std::invalid_argument("replication: '" + options_.listen_address +
                                    "' is not an address to serve replicas from; --replication-ip takes the "
                                    "address alone and --replication-port the port");
    }
    // The device is opened last, once every option is known to be usable: a name
    // that is wrong, or a device that cannot be opened, is a server that refuses
    // to start rather than a replication link that fails later, and an option that
    // is wrong is worth reporting on a machine where no RDMA device exists at
    // all. Every connection the service makes borrows what is opened here.
    if (!options_.rdma_device.empty())
    {
        resources_ = std::make_shared<Core::RdmaResourceManager>(options_.rdma_device);
    }
}

ReplicationService::~ReplicationService() noexcept = default;

void ReplicationService::record(const Command &command)
{
    // Every replica reads the same bytes out of the log, so the encoding is done
    // once, here, in the order the master applied the writes. Nothing waits on a
    // replica: what it has not read is pinned, and a replica that falls off the
    // far end is told rather than served a stream with a hole in it.
    history_.append(command);

    // A stream that found nothing to send is waiting for exactly this. Nothing is
    // handed over here -- whoever wakes looks at its own place in the log -- so a
    // stream that is not waiting pays nothing for the wake-up, and with no
    // replica attached there is nobody to wake: the call itself was 1.6% of the
    // server on every write, which is what asking first avoids.
    if (history_.recording())
    {
        writes_.notify_all();
    }
}

WriteHistory::Cursor *ReplicationService::attach() noexcept
{
    // The snapshot being served ends at the end of the log as it stands here, so
    // that is the offset the replica will be caught up from. The end of the log
    // is always a position the log can be read from, and the offsets go on
    // across a link going away and coming back, so a replica that reconnects can
    // say where it had got to and be answered from there.
    WriteHistory::Cursor *cursor = history_.attach(history_.end_offset());
    spdlog::info("replication: a replica follows from offset {}; the log is now followed by {} reader(s)",
                 history_.end_offset(), history_.cursors());
    return cursor;
}

void ReplicationService::detach(WriteHistory::Cursor *cursor) noexcept
{
    history_.detach(cursor);
    if (!history_.recording())
    {
        spdlog::info("replication: no replica left; the log holds {} commands ({} bytes) from offset {}", history_.commands(),
                     history_.bytes(), history_.oldest_offset());
    }
}

void ReplicationService::prune_sessions()
{
    // A session is only referred to by this list once the coroutine serving it
    // has finished, and its chunks only go back to the pools once it is gone.
    std::erase_if(sessions_, [](const std::shared_ptr<NBIO::RdmaSession> &session) {
        return session.use_count() == 1;
    });
}

Foundation::NBIO::Task<void> ReplicationService::serve()
{
    if (!is_master())
    {
        co_return;
    }

    try
    {
        // The device, its regions and its pools belong to the service, not to this
        // frame: every connection is built from them and every link outlives the
        // accept loop. They were opened when the service was constructed.
        acceptor_.emplace(*resources_);
        const auto bound =
            acceptor_->listen(Core::SocketAddress::from_v4(options_.listen_address, options_.listen_port));
        if (!bound) [[unlikely]]
        {
            spdlog::error("replication: the listener stopped: {}", bound.error());
            co_return;
        }
        spdlog::info("replication: serving snapshots on rdma://{}:{}", options_.listen_address, options_.listen_port);

        NBIO::RdmaAcceptChannel channel(*acceptor_, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
        while (true)
        {
            auto session = co_await channel.accept();
            if (!session) [[unlikely]]
            {
                spdlog::error("replication: the listener stopped: {}", session.error());
                co_return;
            }
            prune_sessions();
            sessions_.push_back(*session);
            NBIO::spawn(serve_replica(std::move(*session)));
        }
    }
    catch (const std::exception &error)
    {
        spdlog::error("replication: the listener stopped: {}", error.what());
    }
}

Foundation::NBIO::Task<void> ReplicationService::serve_replica(std::shared_ptr<NBIO::RdmaSession> session)
{
    try
    {
        // The replica opens the transfer, so this side waits for a peer that is
        // ready before it forks a snapshot nothing may come to collect. What it
        // offers -- its chunk size and how many packets it can hold -- is also
        // what tells this side what it is allowed to send.
        const auto offer = co_await AcceptTransfer(*session);
        if (!offer)
        {
            co_return;
        }

        // Take the snapshot first and start the log immediately after: the
        // snapshot call copies the store before it returns, so the two are
        // adjacent in time and a write can only be in one of them.
        auto pending = host_.snapshot();
        Attachment attachment(*this);
        if (!co_await std::move(pending)) [[unlikely]]
        {
            spdlog::warn("replication: this master could not write a snapshot");
            co_return;
        }

        const auto file = host_.snapshot_file();
        if (file.empty()) [[unlikely]]
        {
            spdlog::warn("replication: this master has no snapshot file to serve");
            co_return;
        }

        // The file the fork just wrote is what travels: no part of the snapshot
        // is held in this process's memory, and the receiver writes it whole or
        // not at all. The packet size is the smaller of the two ends', which is
        // what the offer just said, and the cursor says which log position the
        // image belongs to, so the replica knows where to follow from.
        const auto sent = co_await SendFile(*session, *offer, file, options_.chunk_size, attachment.cursor->offset());
        if (!sent) [[unlikely]]
        {
            spdlog::warn("replication: sending '{}' failed", file.string());
            co_return;
        }

        const auto agreed = std::min<std::uint64_t>(offer->chunk_size, options_.chunk_size);
        spdlog::info("replication: sent '{}' ({} bytes) in {} packets of {} bytes; the log holds {} commands ({} bytes) from offset {}",
                     file.string(), *sent, PacketCount(*sent, agreed), agreed, history_.commands(), history_.bytes(),
                     history_.oldest_offset());

        // The link stays up, and what goes over it from here is the writes this
        // master has applied since the snapshot: one batch per request the replica
        // makes, so the replica sets the pace and the offset it asks from is both
        // its position and its acknowledgement of everything before it. When the
        // log holds nothing past that offset the request waits here, rather than
        // an empty batch going out and coming straight back.
        const std::size_t batch = static_cast<std::size_t>(NegotiatedWindow(*offer)) * agreed;
        while (true)
        {
            const auto request = co_await AcceptTransfer(*session);
            if (!request || request->kind != PayloadKind::kCommands)
            {
                break; // the replica let go, or is asking for something else
            }
            if (!attachment.cursor->valid() || request->offset > history_.end_offset()) [[unlikely]]
            {
                // It wants bytes this log no longer holds, or ones it has not
                // reached: either way it cannot be caught up from here, and it
                // starts again from a snapshot when this link ends.
                spdlog::warn("replication: a replica asked from offset {}, which this log cannot serve", request->offset);
                break;
            }

            // Asking from an offset is acknowledging everything below it, which is
            // what lets those blocks go -- and holding this one keeps the blocks
            // the batch is about to be read out of.
            history_.advance(*attachment.cursor, request->offset);

            const std::uint64_t from = request->offset;
            co_await writes_.wait([this, from, batch] {
                return history_.available(from, batch) != 0;
            });

            const auto size = history_.available(from, batch);
            std::uint64_t position = from;
            const PacketReader read = [this, &position](std::span<char> packet) {
                const auto taken = history_.copy(position, packet);
                position += taken;
                return taken;
            };
            const auto streamed = co_await SendCommands(*session, *request, from + size, size, read, options_.chunk_size);
            if (!streamed) [[unlikely]]
            {
                spdlog::warn("replication: sending {} bytes from offset {} failed", size, from);
                break;
            }
            spdlog::debug("replication: sent {} bytes of writes from offset {}", size, from);
        }
    }
    catch (const std::exception &error)
    {
        spdlog::warn("replication: the replica link failed: {}", error.what());
    }

    spdlog::info("replication: replica link closed");
}

Foundation::NBIO::Task<std::optional<std::uint64_t>> ReplicationService::full_sync(NBIO::RdmaSession &session)
{
    // The master's RDB arrives as a file: it lands beside the RDB path and is
    // moved onto it only once every packet has arrived, so a transfer that dies
    // half way leaves nothing that looks like a snapshot.
    const auto file = host_.snapshot_file();
    if (file.empty()) [[unlikely]]
    {
        spdlog::warn("replication: this server has no RDB file to receive a snapshot into");
        co_return std::nullopt;
    }

    spdlog::info("replication: asking the master for a snapshot");
    const auto received = co_await ReceiveFile(session, file, options_.chunk_size);
    if (!received) [[unlikely]]
    {
        spdlog::warn("replication: receiving '{}' failed", file.string());
        co_return std::nullopt;
    }

    spdlog::info("replication: received '{}' ({} bytes), validating it", file.string(), received->bytes);
    if (!host_.restore(file)) [[unlikely]]
    {
        // Loading is what verifies: an RDB ends with a CRC-64 over its own
        // bytes and a load refuses one whose checksum does not match, so a
        // transfer that lost or damaged a byte cannot become the store.
        spdlog::warn("replication: '{}' did not validate against its own CRC and was not replayed", file.string());
        co_return std::nullopt;
    }

    spdlog::info("replication: replayed '{}', which is the log up to offset {}", file.string(), received->offset);
    co_return received->offset;
}

Foundation::NBIO::Task<void> ReplicationService::follow_master(NBIO::RdmaSession &session, std::uint64_t offset)
{
    // The incremental half, one batch at a time: ask from what has been applied,
    // apply what comes back, ask again. The request carries the position, so the
    // master knows both where to cut the log and that everything before it is
    // done with and can be let go.
    //
    // The decode is kept across batches: a batch ends wherever the log did, which
    // is as likely to be the middle of a command as between two, and the decoder
    // is what holds the half of it that has arrived.
    Foundation::Core::Buffer buffer(options_.chunk_size * options_.chunk_count * 2,
                                    options_.chunk_size * options_.chunk_count * 8);
    auto decoder = RESP::Decode(buffer);

    while (true)
    {
        const PacketWriter write = [&buffer](std::span<const char> packet) -> bool {
            return buffer.write(packet.data(), packet.size());
        };
        const auto batch = co_await ReceiveCommands(session, offset, write, options_.chunk_size);
        if (!batch) [[unlikely]]
        {
            spdlog::warn("replication: the master stopped sending writes at offset {}", offset);
            co_return;
        }

        while (decoder.poll() == RESP::DecodeStatus::kComplete)
        {
            const auto &decoded = decoder.result();
            const KV::CommandValidation validation =
                decoded.object ? KV::ValidateCommand(*decoded.object) : KV::CommandValidation{};
            if (!validation || !host_.apply(*validation.command)) [[unlikely]]
            {
                spdlog::warn("replication: '{}' could not be applied", decoded.error.empty() ? validation.error : decoded.error);
                co_return;
            }
            // The bytes of a command are what the master's log holds for it, so
            // counting them is what keeps this side's offset the same number the
            // master's is. Whatever is left over is not a whole command yet, and
            // asking for it again is what the next request does.
            offset += KV::EncodeCommand(*validation.command).size();
            decoder = RESP::Decode(buffer);
        }

        // The master says where its log was when it cut the batch. Everything
        // below that this side has applied, and what is left is the part of a
        // command that is still arriving -- never more than the batch itself.
        if (batch->offset < offset || batch->offset - offset > batch->bytes) [[unlikely]]
        {
            spdlog::warn("replication: the master's log ends at {} and this replica is at {}", batch->offset, offset);
            co_return;
        }
    }
}

Foundation::NBIO::Task<void> ReplicationService::follow()
{
    if (!is_replica())
    {
        co_return;
    }

    const std::string master = Endpoint(*options_.master);
    while (true)
    {
        bool linked = false;
        try
        {
            // A fresh connector per attempt: a successful connect hands its
            // communication id to the session, so it cannot be reused. The device
            // it borrows is the service's, opened once at startup.
            link_ = std::make_unique<ReplicaLink>(resources_);
            NBIO::RdmaConnectChannel channel(link_->connector, NBIO::Engine::multiplexer(), NBIO::Engine::scheduler());
            auto session = co_await channel.connect(*options_.master);
            if (!session) [[unlikely]]
            {
                // Connecting used to throw and land in the handler below; the
                // failure is a value now, so it is reported in the same words
                // and the retry that follows is the same one.
                spdlog::warn("replication: the link to {} failed: {}", master, session.error());
            }
            else
            {
                link_->session = std::move(*session);

                if (const auto offset = co_await full_sync(*link_->session))
                {
                    linked = true;
                    host_.link_changed(true);
                    spdlog::info("replication: following master {}", master);
                    co_await follow_master(*link_->session, *offset);
                    spdlog::warn("replication: the link to {} ended", master);
                }
            }
        }
        catch (const std::exception &error)
        {
            spdlog::warn("replication: the link to {} failed: {}", master, error.what());
        }

        if (linked)
        {
            host_.link_changed(false);
        }

        link_.reset();
        co_await NBIO::sleep_for(std::chrono::seconds(1));
    }
}
} // namespace KV

#endif // defined(__linux__)

