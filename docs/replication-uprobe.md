# Logical write-log uprobe replication (draft)

## Status

This draft replaces the previous mmap/eventfd command queue. The default transport is
still RDMA for both full and incremental synchronization. Setting
`--replication-daemon IPv4:port` opts into external uprobe capture and daemon TCP
incremental replication. The master launches `repld` during startup and shares
only replication metadata through mmap; write payloads still travel via uprobe.

The server, daemon, and BPF object build. Tests cover the logical hook arguments,
binary payloads, command sequences, fork isolation, chunk assembly, loss recovery, NBIO transport,
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
           increment global write counter; encode once if logging is needed
           call kvstore_write_log_v1(epoch, end_offset, bytes, length)
                |
                +-- uprobe --> BPF ring --> ReplicationDaemon
                |                            capture, backlog, NBIO TCP followers
                |
                +-- AOF enabled --> append the same bytes to disk

Master -- tuple at counter S; fork without suspension --> snapshot child
Replica -- FOLLOW epoch S ticket --> repld
Replica <-- +CONTINUE epoch -- daemon
Replica buffers incremental commands while RDMA transfers the forked dump
Replica restores dump, sends FULLSYNCED, then releases RDMA and replays backlog
```

With AOF disabled in daemon mode, write logging is skipped whenever the shared
atomic `ReplicationInfo::replica_count` is zero, including after the last replica
detaches. Skipped writes neither encode nor allocate the encoding buffer nor
invoke the payload hook. They still increment the global write counter.
The next snapshot covers those writes.
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

The monolithic `ReplicationDaemon` class owns probe attachment, event assembly,
the replay backlog, follower connections, progress, and NBIO fanout. The
daemon-enabled master owns no per-follower incremental queues or ACK state.
Transient RDMA snapshot sessions and bounded shared handoff slots remain on the master.
Without a configured
daemon, the existing RDMA incremental queues and delivery path remain active.

## Shared metadata and replica-count ownership

`ReplicationInfo` is a versioned struct mapped `MAP_SHARED` from a sealed memfd.
The master creates it and passes the descriptor to its `repld` child at exec.
Both processes must use the same ABI and lock-free 64-bit atomics. It currently
holds the replica count, daemon/capture readiness, master write count, captured
command count, and 128 generation-tagged handoff slots;
future shared fields belong in this struct, with an appropriate ABI update.
It is **not** a shared command buffer.

Count ownership is deliberately asymmetric:

1. **The master increments** once when an RDMA replica begins snapshot
   synchronization, before forking its snapshot.
2. **The master decrements** if the snapshot or connection to `repld` fails
   before handoff. Capture-boundary confirmation is bounded at five seconds.
3. **`repld` never increments.** A valid FOLLOW atomically claims the existing
   ticket before `+CONTINUE`, inheriting the counted connection from RDMA
   while the full snapshot is still transferring.
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

extern "C" void kvstore_replication_start_v1(
    uint64_t epoch, uint64_t offset, uint64_t ticket) noexcept;
```

These synchronous functions have unmangled versioned symbols, noinline/retention
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

Epochs change on process/log construction, not on AOF enable/disable. Offsets
are now **command counts, not byte offsets**. At boundary S the dump contains
the first S successful logged write operations. The next command has zero-based
index S and carries exclusive end counter S+1 on the wire. SET and DEL each
advance the counter once, regardless of payload length. Borrowed SETs count too,
even when no encoding is needed. Startup bulk load/AOF replay does not count.
This changes the replication wire semantics; do not mix old/new binaries.

The AOF's local write counter must equal `ReplicationInfo::write_count` before
each increment. In `ReplicationDaemon::AppendCommand`, capture assembly checks
that commands increment by exactly one and never exceed the shared count.
After consuming a stable BPF status revision, the append-command head must equal
the producer's committed head. `captured_count` can legitimately lag the master
while capture is in flight; unequal asynchronous samples alone are not failure.

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

The tuple hook arms BPF capture synchronously at S before fork; it does not wait
for daemon userspace scheduling. Initial polling uses this saved **baseline**,
not the later observed head, preserving commands already queued after the tuple.
Further tuples at a continuous head do not reset existing followers.
On capture loss, the daemon anchors at a new baseline and discards partial history. Existing followers are
disconnected and must resnapshot. Queued events at/before that baseline are
ignored. A new snapshot at a retained boundary can start following again.

The failure counter cannot prove that a wrong binary, an uninstrumented write
path, or externally removed probe was observed. Heartbeats detect inactivity,
not arbitrary instrumentation mistakes. Verify the symbol and all write paths
for each supported build.

## Snapshot handoff and offset chasing

The master prepares the tuple at S, calls the deliberate tuple hook, and
immediately forks the live store on the same single-threaded executor, with
**no await between these operations**. The child captures and serializes its
copy-on-write view; the parent does not first copy the store into a vector.
It closes inherited master descriptors and never runs the inherited NBIO
runtime. The parent uses NBIO timers and nonblocking waitpid to observe completion;
cancellation kills and reaps the owned child. A parent-death signal also kills it.
This path assumes the master is single-threaded; it is not a general safe
post-fork C++ serializer for an arbitrarily multithreaded host.
Existing background saves use the same fork-and-NBIO-wait helper rather than
creating a helper thread, preserving this single-threaded parent model.

The master confirms that daemon userspace observed the capture boundary before
publishing the tuple on RDMA. Network send completion is not the snapshot
boundary: waiting for a send and only then forking would allow intervening writes.
Snapshot sessions are serialized and use `replication.rdb`, separate from
SAVE/BGSAVE's `dump.rdb`.

The daemon-mode exchange is:

```text
replica -> master  PSYNC ? -1
master  -> replica +REPLICATION epoch S IPv4:port ticket\r\n
                   (tuple hook and fork happen before asynchronous publication)
replica -> daemon  FOLLOW epoch S ticket\r\n
daemon  -> replica +CONTINUE epoch\r\n
master  -> replica +FULLSYNC\r\n$length\r\n<RDB bytes>
daemon  -> replica commands accumulate in the replica's backlog in parallel
replica loads and validates RDB
replica -> master  FULLSYNCED epoch S (RESP array)
master  -> replica +CONTINUE epoch\r\n
replica releases RDMA, drains its backlog, then keeps applying new commands
```

The daemon accepts only an exact retained command boundary in the same epoch
and an unclaimed, pending master-issued ticket.
It then streams:

```text
@epoch end_offset payload_length\r\n<payload>
```

A replica at S accepts a nonempty record only when
`end_offset == S + 1`, with overflow checks. A heartbeat has zero
length and must quote S exactly. Malformed payloads, failed application, gaps,
epoch mismatch, disconnection, or heartbeat timeout trigger another full sync.

The replica's receiver and snapshot loader run concurrently using NBIO.
No buffered command is applied until snapshot restore and FULLSYNCED confirmation.
Its 64 MiB backlog includes per-record overhead; overflow explicitly aborts the
attempt rather than growing without bound or skipping commands. A connection,
FOLLOW, framing, or snapshot failure cancels the sibling tasks and closes both
paths. There is no fallback to RDMA incremental mode in this handshake.
`INCREMENTAL` is reserved as a possible future alternative, **not implemented**.
The outer follow loop may retry a fresh full-sync request after failure.

Without `--replication-daemon`, the existing standalone RDMA incremental mode
still uses FULLRESYNC/PSYNC. It also reports command counts in snapshot metadata.

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

The daemon lives in its own `repld/` directory and builds as a standalone
executable (target `repld`); the rest of the tree does not depend on libbpf.

```bash
cmake -S . -B build
cmake --build build --config Release --target server repld
```

```bash
# Run the master. It prints the repld command line and waits for it, because
# only the daemon needs BPF attachment privileges.
./build/applications/Release/server \
  --port 6666 --rdma-device siw0 \
  --replication-ip 192.168.4.146 \
  --replication-daemon 192.168.4.146:6668 --repld-manual
# In another shell, run the command the master printed, e.g.:
sudo ./build/applications/Release/repld \
  --pid <master-pid> --binary ./build/applications/Release/server \
  --info-file /tmp/kvstore-repld-<master-pid>.info \
  --listen-address 192.168.4.146 --port 6668 --multiplexer epoll

./build/applications/Release/server --port 7777 --rdma-device siw0
redis-cli -p 7777 SLAVEOF 192.168.4.146 6666
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
  -R '^(ForkSnapshot|ReplicationInfo|LogicalLog|CaptureStream|ReplayBacklog|ReplicationProtocol|Backends/DaemonTransportTest)' \
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
history loss, and shutdown. They also verify buffering before snapshot completion,
ordered catch-up followed by new writes, bounded-backlog failure, and rejected
FOLLOW admission. Fork tests verify that parent mutations do not alter the dump.

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
