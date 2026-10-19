# Logical write-log uprobe replication (draft)

## Status

This draft replaces the previous mmap/eventfd command queue. The default transport is
still RDMA for both full and incremental synchronization. Setting
`--replication-daemon IPv4:port` opts into external uprobe capture and daemon TCP
incremental replication. The master launches `repld` during startup and shares
only replication metadata through mmap; write payloads still travel via uprobe.

The server, daemon, and BPF object build. Tests cover the logical hook arguments,
binary payloads, byte offsets, chunk assembly, loss recovery, NBIO transport,
RDMA fallback, and AOF enable/disable. Release disassembly confirms the hook
symbol and real calls. Live BPF loading in the development environment fails
with EPERM, so kernel verifier acceptance and real privileged end-to-end capture
are **not yet verified**. Do not treat this as production-ready replication.

The previous mmap attempt is preserved in the Git stash named
`preserve mmap replication attempt before logical-log uprobe draft`.
Unrelated worktree changes are retained.

## Architecture

```text
Successful SET/DEL, without suspension between mutation and logging
    |
    +--> AppendOnlyFile::log(command)
           encode once; advance epoch-scoped logical byte offset
           call kvstore_write_log_v1(epoch, end_offset, bytes, length)
                |
                +-- uprobe --> BPF ring --> CaptureStream --> replay backlog
                |                                               |
                |                                          NBIO TCP followers
                |
                +-- AOF enabled --> append the same bytes to disk

Master -- RDMA snapshot at offset S --> replica
Replica -- FOLLOW epoch S ticket --> repld
Replica <-- +CONTINUE epoch -- daemon
Replica releases its RDMA connection and follows TCP only
```

With AOF disabled in daemon mode, write logging is skipped whenever the shared
atomic `ReplicationInfo::replica_count` is zero, including after the last replica
detaches. Skipped writes neither encode nor allocate the encoding buffer, advance
the offset, nor invoke the payload hook. The next snapshot covers those writes.
Snapshotting replicas are counted before capture, so writes during transfer are
not skipped. AOF-enabled writes are always logged regardless of replica count.
Zero-length liveness hooks still run every 100 ms while idle.

The legacy RDMA-only path retains first-snapshot activation: after its first
replica it keeps logging even if all RDMA followers disconnect.

Encoding reuses one lazily allocated fixed-capacity buffer (16 MiB + 64 KiB).
The returned view is valid only until the next log call. The synchronous probe
and RDMA queue copies consume it immediately. Disk append copies into its own
buffer before its first suspension, so another client cannot overwrite an
in-flight AOF write. The encoding buffer does not grow per command.

The daemon owns follower connections, retention, progress, and fanout. The
daemon-enabled master owns no per-follower incremental queues or ACK state.
Transient RDMA snapshot sessions and bounded shared handoff slots remain on the master.
Without a configured
daemon, the existing RDMA incremental queues and delivery path remain active.

## Shared metadata and replica-count ownership

`ReplicationInfo` is a versioned struct mapped `MAP_SHARED` from a sealed memfd.
The master creates it and passes the descriptor to its `repld` child at exec.
Both processes must use the same ABI and lock-free 64-bit atomics. It currently
holds the count, daemon readiness, and 128 generation-tagged handoff slots;
future shared fields belong in this struct, with an appropriate ABI update.
It is **not** a shared command buffer.

Count ownership is deliberately asymmetric:

1. **The master increments** once when an RDMA replica begins snapshot
   synchronization, before waiting for/capturing its snapshot.
2. **The master decrements** if the snapshot or connection to `repld` fails
   before handoff, including a five-second FOLLOW handoff timeout.
3. **`repld` never increments.** A valid FOLLOW atomically claims the existing
   ticket before `+CONTINUE`, inheriting the counted connection from RDMA.
4. **`repld` decrements** once when an inherited follower detaches, times out,
   loses retained history, encounters a transport failure, or is stopped.

A bare counter cannot arbitrate a failed-handoff race: the master might otherwise
decrement just as the daemon accepts and later decrements the same connection.
Pending/owned ticket states make claim versus cancellation a CAS race with one
winner. Generation tags reject duplicate FOLLOWs and stale tickets after slot
reuse. After claim, master cleanup cannot decrement; before claim, daemon cleanup
cannot decrement. Cancelling follower coroutines also releases owned tickets.
The count includes pending snapshots as well as active TCP followers, and the
128-slot cap is explicit rather than silently dropping replication.

## Stable hook ABI

```cpp
extern "C" void kvstore_write_log_v1(
    uint64_t epoch,
    uint64_t end_offset,
    const char* payload,
    uint64_t length) noexcept;
```

This synchronous function has an unmangled versioned symbol, noinline/retention
attributes, and an observable compiler barrier to preserve its calls under
optimization. The pointed-to bytes remain valid throughout the call. The BPF
program copies them synchronously and never stores pointers for later use.
No decoding of `std::string`, `std::variant`, or coroutine frames is needed.

A zero-length call reports the current position and liveness; it is not a write.
Daemon-enabled masters call this every 100 ms, even when idle.

The draft assumes one serialized logging thread. BPF records its thread identity
and marks capture failed on another thread, rather than claiming a total mutation
order for concurrent writers. SET/DEL mutation and logical logging do not await.
The borrowed SET shortcut remains available while logging is inactive, then
routes through the owning command/log path so it cannot bypass capture. DEL is
synchronous for the same mutation-to-log boundary.
Bulk load/AOF startup replay is not emitted as incremental writes; it belongs in
the initial snapshot.

Epochs change on process/log construction, not on AOF enable/disable. Logical
offsets count canonical RESP bytes, excluding AOF checksum trailers. They are
not physical file offsets or fsync positions.

## Capture and loss

The uprobe attaches to one PID and the exact server executable using libbpf.
It copies records in 4 KiB chunks into a bounded 64 MiB BPF ring. The maximum
captured record is 16 MiB; larger writes invalidate incremental capture and
require a snapshot. Userspace holds at most one partial record.

An independently polled BPF status map contains epoch, logical head, failure
counter, owner thread, and monotonic observation time. A revision counter
protects the producer metadata tuple; unstable reads defer work.

The producer increments the failure counter on failed ring reservation, failed
user-memory read, unsupported record length, or discontinuous offset. Thus a
lost **final** write is detected without waiting for a later ring event.
Attachment failure, map errors, and stale/unstable status lasting five seconds
are explicit daemon errors, not successful fallback.

On initial attachment or capture loss, the daemon anchors at the observed
committed head and discards old/partial history. Existing followers are
disconnected and must resnapshot. Queued events at/before that baseline are
ignored. A new snapshot at a retained boundary can start following again.

The failure counter cannot prove that a wrong binary, an uninstrumented write
path, or externally removed probe was observed. Heartbeats detect inactivity,
not arbitrary instrumentation mistakes. Verify the symbol and all write paths
for each supported build.

## Snapshot handoff and offset chasing

Snapshot capture copies live state synchronously on the same executor as writes.
The master records logical offset S before yielding to background serialization.
Snapshot sessions are serialized and use `replication.rdb`, separate from
SAVE/BGSAVE's `dump.rdb`.

The existing full-sync exchange remains:

```text
replica -> master  PSYNC ? -1
master  -> replica +FULLRESYNC epoch S\r\n$length\r\n<RDB bytes>
replica loads and validates RDB
replica -> master  PSYNC epoch 0
master  -> replica +DAEMON IPv4:port epoch S ticket\r\n
replica -> daemon  FOLLOW epoch S ticket\r\n
daemon  -> replica +CONTINUE epoch\r\n
```

The daemon accepts only an exact retained command boundary in the same epoch
and an unclaimed, pending master-issued ticket.
It then streams:

```text
@epoch end_offset payload_length\r\n<payload>
```

A replica at S accepts a nonempty record only when
`end_offset == S + payload_length`, with overflow checks. A heartbeat has zero
length and must quote S exactly. Malformed payloads, failed application, gaps,
epoch mismatch, disconnection, or heartbeat timeout trigger another full sync.

After daemon acceptance, the replica stops the RDMA reader and releases its
snapshot connection. TCP connect alone is insufficient. If attachment or FOLLOW
fails, the replica retries full sync; there is no silent transport switch.

This is Redis-like offset chasing, not a durable Redis replication journal:
only ranges retained by the daemon can be replayed. Capture loss has no
guaranteed disk/AOF repair path, even when AOF is enabled.

## Process lifecycle

The master waits up to five seconds for its child to attach the probe and bind
the TCP listener. Missing executables, missing BPF privileges, occupied ports,
and early exits fail startup explicitly; there is no silent RDMA fallback.
The default executable is `repld` beside `server`; `--repld PATH` overrides it.
The daemon inherits the master's `--multiplexer` choice.

While running, the master checks its child every 100 ms. Unexpected daemon exit
is fatal to the configured master (exit status 1), rather than continuing with
stale counts or unverifiable replication. There is no automatic daemon restart.
Restart the master and resynchronize replicas; a fresh process gets fresh shared
metadata and an epoch. Graceful daemon stop releases its owned tickets, but
SIGKILL cannot run cleanup, which is why the master must not keep serving.
Normal owner destruction terminates and reaps the child, with a bounded
SIGTERM-to-SIGKILL fallback. Linux's parent-death signal also terminates `repld`
if the master process dies without running destructors.

## Build and run

Linux, a BPF-capable clang, libbpf development headers/library (via pkg-config),
and the existing RDMA dependencies are required. No SystemTap headers, kernel
BTF dump, or generated libbpf skeleton is needed.

```bash
cmake -S . -B build -DKVSTORE_BUILD_PROBE_DAEMON=ON
cmake --build build --config Release --target server repld
```

For a server/RDMA-only build, set `KVSTORE_BUILD_PROBE_DAEMON=OFF`; no BPF build
tools or libbpf are required in that mode.

```bash
# Run the master with the BPF/perf attachment privileges required by its child.
./build/applications/Release/server \
  --port 6666 --rdma-device siw0 \
  --replication-ip 192.168.4.146 --replication-port 6667 \
  --replication-daemon 192.168.4.146:6668

./build/applications/Release/server --port 7777 --rdma-device siw0
redis-cli -p 7777 SLAVEOF 192.168.4.146 6667
```

No separate daemon launch is needed. Its endpoint must be a local numeric IPv4
address reachable by the replicas, since the master both binds and advertises it.
`repld` requires the inherited `--info-fd` and must be a child of its target master;
its low-level `--pid`, `--binary`, and `--info-fd` arguments are supplied by the
launcher. It defaults to the BPF object in its build tree; install a matching
object there for deployment (the low-level `--bpf-object` overrides it).
The daemon and object must have matching versions.
Both epoll and io_uring TCP backends are available.

## Validation

```bash
cmake --build build --config Release --target replication_testing replication_daemon_testing
ctest --test-dir build -C Release \
  -R '^(ReplicationInfo|LogicalLog|CaptureStream|ReplayBacklog|ReplicationProtocol|Backends/DaemonTransportTest)' \
  --output-on-failure

KVSTORE_RDMA_ADDRESS=192.168.4.146 \
  ctest --test-dir build -C Release -R '^replication_network_' --output-on-failure

# Explicit privileged opt-in; an attachment failure fails rather than skips.
KVSTORE_TEST_UPROBE=1 KVSTORE_RDMA_ADDRESS=192.168.4.146 \
  ctest --test-dir build -C Release -R '^replication_probe_network_' --output-on-failure
```

Unprivileged tests cover count ownership, failed/duplicate/stale FOLLOWs,
cross-process claim/cancel races, count-driven logging, inherited descriptor
mapping, child readiness/termination, and unexpected child exits. A dedicated
test fixture checks process supervision without pretending to capture writes.
Transport tests exercise both NBIO backends and release counts on disconnect,
history loss, and shutdown.

Privileged scenarios run the real master, auto-started daemon, and two replicas,
checking binary SET/DEL, large records, RDMA descriptor release, and fail-fast
supervision after killing `repld`. The master and child backend vary together.
Legacy RDMA-only fixtures stay on epoll. Without opt-in these two
tests are explicitly skipped, not counted as verified capture.

## Limits

- Ordered-prefix-or-resnapshot semantics only, with asynchronous lag.
- No durable replicated acknowledgment, RYW token, transaction-group atomicity,
  leader election, fencing, or automatic AOF range recovery.
- Only supported SET/DEL effects are replayed. TTL/eviction effects are not
  replicated; LRU is rejected on daemon-enabled masters and replicas.
- Persistent capture overload can prevent catch-up.
- The TCP replication port is unauthenticated and unencrypted; use a trusted,
  restricted replication network.
- Loading BPF can require kernel capabilities and resource limits unavailable
  in containers or restricted user sessions. The current draft still requires
  privileged verifier and runtime validation.
- An expanded 70 KiB legacy RDMA incremental scenario timed out during testing,
  although isolated comparisons with the preserved original and this draft both
  passed. Its reliability is not established by the smaller RDMA fallback tests.
  Large canonical AOF records and TCP daemon transport are tested separately.
