# Proposal: ordered-effect replication with eBPF-assisted delivery

Status: historical architectural proposal. The recoverable master journal below
is not implemented. The current logical write-log uprobe draft is described in
[the implementation guide](replication-uprobe.md). It uses external capture,
daemon-owned retention, and full resynchronization on loss.

This document specifies a replication model for KVStore in which the master
publishes one authoritative change stream and `replicationd` manages replicas.
eBPF accelerates observation and delivery; it is not the source of truth.

The former XDP, USDT, and mmap prototypes are superseded by the logical-log draft.
The existing RDMA incremental service remains available when no daemon endpoint
is configured. Neither mode implements all of the guarantees proposed below.

## 1. Goals and non-goals

Goals:

- Remove per-replica connections, queues, acknowledgments, and retries from the
  master's normal incremental replication path.
- Preserve the master's mutation order and provide a precise snapshot boundary.
- Detect and recover from lost eBPF events without silently skipping changes.
- Support asynchronous replicas and optional read-your-writes (RYW).
- Keep memory bounded and specify behavior when a consumer cannot keep up.
- Support both epoll and io_uring without depending on socket syscall hooks.

Non-goals for the initial design:

- Consensus, automatic leader election, or split-brain resolution.
- Zero application changes or zero replication overhead on the master.
- Linearizable reads from arbitrary asynchronous replicas.
- Durable acknowledged writes or lossless failover without additional logging
  and acknowledgment requirements.
- Maintaining exact equality of internal cache structures such as LRU lists.

## 2. Why ingress mirroring is insufficient

Packets and receive completions identify input, not applied changes. Commands
can span receives, multiple commands can share a receive, and coroutine yields
can interleave execution across connections. Failed writes, conditional writes,
transactions, expiration, and eviction further separate input from effects.

Grouping captured traffic by connection preserves framing only if capture also
provides a reliable byte stream. It does not establish cross-connection mutation
order or an exact snapshot boundary.

An incomplete-command counter indicates when a tracked set has drained. It does
not identify per-connection replay offsets or account for all pending semantic
state, such as transaction queues. This proposal replaces that mechanism with
an ordered applied-effect stream.

## 3. Architecture and ownership

```text
Client -> parse/validate -> ordered mutation application -> response
                                  |
                         master-owned journal
                                  |
                       stable application probe
                                  |
                             eBPF event
                                  |
                             replicationd
                             /          \
                        replica A    replica B

replicationd -- range recovery / snapshot request --> master
```

| Component | Responsibilities |
| --- | --- |
| Master | Order mutations, record effects, retain a bounded journal, establish snapshot boundaries, serve journal recovery |
| eBPF | Observe a stable application probe and emit bounded delivery hints or payload copies |
| `replicationd` | Consume/recover the stream, retain backlog, distribute snapshots, manage replicas and their progress |
| Replica | Install a snapshot, apply a contiguous stream, acknowledge application progress, gate reads |

The master tracks one daemon-consumption position, not one position per replica.
Additional daemons or daemon high availability require an explicit retention
and acknowledgment policy and are outside the initial scope.

## 4. Stream identity and records

A position is `(epoch, sequence)`. An epoch is an opaque, unique stream
generation, changed on restart or leadership transition unless durable recovery
can safely resume the same stream. Sequences increase monotonically within an
epoch and are never reused or allowed to wrap.

A conceptual record contains:

```text
protocol_version
epoch
sequence
payload_length
effects[]
integrity_check
```

This is a logical schema, not a native C structure or finalized wire encoding.
The wire format must define byte order, size limits, integrity validation, and
version negotiation. A checksum detects accidental corruption; it is not peer
authentication.

One record represents one atomic mutation unit. A transaction that is atomic on
the master is one atomic replay group. Failed operations with no state changes
need no record; any side effects that did occur must still be recorded.

Effects are materialized changes rather than commands to reevaluate:

| Master operation | Example effect |
| --- | --- |
| Successful conditional SET | Put the resulting value and expiration |
| INCR | Put the resulting numeric value |
| Delete, expiration, or eviction | Delete the identified key |
| Change expiration | Set the resulting absolute expiration timestamp |

Include all state needed to reproduce externally visible data behavior.
Replica application must not rerun conditional decisions, local eviction, or
other nondeterministic command logic. A replica that cannot retain required data
must report failure and stop advancing rather than silently evicting it.

Absolute expirations avoid extending TTLs by transport delay, but do not remove
clock-skew problems. Define clock assumptions and local expiry behavior before
claiming time-sensitive read equivalence. Independent physical cleanup must not
change ordered replay semantics.

## 5. Mutation and journal publication

The master provides an ordering/commit boundary shared by every mutation path,
including optimized SET paths, transactions, expiration, and eviction.

For each mutation unit:

1. Prepare the outcome and reserve sufficient journal capacity. Operations that
   discover effects during application need staging or rollback support.
2. In a serialized mutation section, assign the next sequence, apply the effects,
   and publish the complete immutable journal record.
3. Advance the committed journal head only after application and publication
   are complete.
4. Invoke the stable probe identifying the committed record.
5. Return client success only after the required acknowledgment policy is met.

Readers, snapshots, and other mutations must not observe partially applied
groups or an inconsistent state/journal boundary. A single-threaded executor can
use a non-suspending section; concurrent writers require explicit coordination.
I/O waits and client backpressure do not belong inside that section.

Publication must not fail after an irreversible mutation in a way that allows
normal operation to continue with an unrecorded change. Reserve resources first;
otherwise use rollback or stop serving and invalidate the affected stream.
Durable crash atomicity requires a separate write-ahead/recovery protocol.

The default acknowledgment means applied and committed to the memory journal,
not durable on disk or applied by a replica.

## 6. eBPF delivery and gap recovery

Prefer a stable, versioned USDT-style application probe or explicit supported
probe ABI over probing private functions and hard-coded structure offsets.
It fires only after the journal record is committed.

The simplest event is `(epoch, sequence)`, identifying a record available through
the journal reader. Payload copying is an optional optimization to benchmark.
Probe arguments or transient pointers are not an ownership transfer: payloads
must be copied synchronously or fetched through a lifetime-safe journal API.

`replicationd` maintains a contiguous consumed position C:

- Position C+1: validate and consume, then advance C.
- Position greater than C+1: recover the missing range before advancing.
- Position at or below C: discard a matching duplicate; conflicting content is
  a protocol failure.
- Different epoch: stop applying the old stream and negotiate recovery.

Ring-buffer reservation failure, probe detachment, and daemon stalls are
expected failures. Event order is not authoritative; sequence order is.
Periodically query the committed head even when no events arrive, so a lost
final event is detected without waiting for a later write.

Journal reads must validate epoch and retained range and copy or pin records
against reclamation. Return an explicit unavailable-range error when recovery
is impossible. Do not substitute an empty successful response.

Expose capture-drop counters, recovered gaps, oldest retained sequence,
committed head, and daemon lag. Capture failures must never be silent.

## 7. Retention and backpressure

The master journal is bounded. The daemon acknowledges C only after accepting
responsibility for records through C. If it promises restart recovery, this
requires durable daemon storage before acknowledgment.

If the daemon uses only memory, its restart can require replica resynchronization
after master records have been reclaimed. This is an availability limitation,
not permission to omit records.

When capacity is exhausted, support an explicit policy:

- Correctness-first: delay or reject new writes before mutation until capacity
  is available.
- Availability-first: expire retention and invalidate lagging recovery sessions,
  requiring a fresh snapshot.

The policy must be selected explicitly. A snapshot session must retain its
required range or fail explicitly; no protocol can guarantee catch-up with
unbounded lag and bounded storage.

Slow replicas consume daemon resources, not per-replica master queues. The daemon
may disconnect and resynchronize replicas that exceed its retention budget.

## 8. Snapshot and incremental handoff

The core invariant is:

> Snapshot S represents committed effects through S; replay starts at S+1.

Protocol:

1. The daemon requests a snapshot session from the master.
2. At the mutation boundary, atomically capture the committed position S,
   establish an immutable snapshot view, and protect the replay range after S.
3. Resume mutation application once the view is safe. Serialize and transfer
   the snapshot while the daemon collects later records.
4. Transfer a manifest containing epoch, S, format version, size, integrity
   information, and relevant expiration semantics.
5. The replica validates and loads the snapshot into an isolated store, then
   atomically installs it with applied position S.
6. The replica applies S+1 onward, acknowledging only contiguous application.
7. Release the master's snapshot retention protection when the daemon has
   accepted responsibility for the needed range. Individual replica ACKs remain
   the daemon's responsibility.

A bounded copy, fork/COW, or versioned snapshot can establish the immutable view;
the choice must fit KVStore's threading, allocator, and RDMA constraints. Merely
starting background serialization does not establish a consistent view.

Snapshot failure, epoch changes, or lost retention abort the session. A replica
must not serve a partially loaded image as synchronized data. RDMA is a transfer
mechanism; it does not itself provide snapshot consistency or application ACKs.

The master-daemon control channel may remain connected. Removing master-side
replica state does not require eliminating this one publisher relationship.

## 9. Replica state machine

```text
DISCONNECTED -> NEGOTIATING -> LOADING_SNAPSHOT -> CATCHING_UP -> STREAMING
                       \---- retained-range resume -----------/
Any state -> RESYNC_REQUIRED on unrecoverable gap or epoch mismatch
```

Track received position separately from applied position. Only the latter
establishes read visibility. Apply each atomic group completely before advancing
the applied position or exposing its changes to readers.

Within a live epoch, an applied-position check suppresses duplicate delivery.
Across replica crashes, the dataset and applied position must recover together;
otherwise discard the recovered claim and reload a snapshot. Absolute-value
effects do not make arbitrary duplicate replay safe across newer records.

Application errors, invalid records, or conflicting duplicates stop progress and
surface an explicit error. Never acknowledge failed application.

## 10. Consistency and read-your-writes

Under valid leadership, complete effect capture, successful recovery, and the
defined expiration policy, a replica's logical data corresponds to an ordered
prefix of the master's committed effect stream. This is asynchronous replication,
not arbitrary reordering or a promise of equal wall-clock observations.

Two RYW modes are possible:

1. Master reads: route the client's subsequent reads to the same authoritative
   master after its successful write. This needs no new replication token.
2. Token-gated replica reads: return `(epoch, sequence)` for the write; execute
   a subsequent read only when the serving replica has applied through that
   position in the same epoch. Otherwise wait with a deadline, route to the
   master, or return an explicit consistency-unavailable error.

The visibility check and read must use the same store generation; a concurrent
snapshot replacement or epoch switch must not invalidate the check.
Later legitimate overwrites or expirations can supersede the client's value;
RYW does not mean that value remains forever.

Tokens are not numerically comparable across epochs. A failover cannot claim
RYW for an old token without proving its history survived.

## 11. Durability, failover, and trust

| Failure | Required response |
| --- | --- |
| Lost eBPF event | Recover from journal; resnapshot if unavailable |
| Daemon restart | Resume from retained/durable position or resnapshot |
| Replica disconnect | Resume from applied position if retained |
| Master restart with memory-only journal | New epoch; acknowledged writes may be lost |
| Snapshot integrity failure | Reject image and retry/resynchronize |
| Replica application failure | Stop, report error, do not advance ACK |
| Concurrent masters | External leadership and fencing required |

Epoch identifiers detect stream changes but do not fence stale masters.
Durable acknowledgment requires a specified storage flush policy; replica
acknowledgment requires defining which replicas, whether application or
persistence is required, and what failures can still lose acknowledged data.
Neither is supplied by eBPF.

Authenticate and authorize snapshot, journal, and replica control endpoints.
Replication data can contain every cached value; protect it according to the
deployment threat model. Scope BPF attachment to the intended process and probe
version, validate record sizes, and restrict journal access. RDMA is not an
authentication or confidentiality guarantee by itself.

## 12. Implementation stages and acceptance criteria

Stages:

1. Implement ordered effects, journal recovery, and snapshot S/S+1 handoff with
   a direct notification path. Establish correctness before introducing eBPF.
2. Add the stable probe and eBPF consumption with the same recovery protocol.
3. Move replica lifecycle and backlog ownership into `replicationd`.
4. Add token-gated reads and optional durable acknowledgment policies.
5. Benchmark direct notification against eBPF hints and payload copies. Retain
   eBPF only where it provides a measured operational or performance benefit.

Required tests:

- Concurrent conflicting writes and pipelines crossing fairness yields and
  response-backpressure suspension points on epoll and io_uring.
- Fragmented requests and open transactions across snapshot establishment.
- Conditional failures, increments, deletes, expirations, evictions, and atomic
  transaction groups with no missing fast-path mutations.
- Snapshot at S under continuing writes: replay S+1 through N and compare
  canonical logical state against the master at N using controlled time.
- Forced ring overflow, detached probes, reordered/duplicate notifications, and
  a lost final event followed by no further writes.
- Journal wrap, oversized effects, exhausted capacity, and slow snapshot loads:
  verify explicit backpressure or resynchronization, never silent omission.
- Master, daemon, and replica restarts at protocol boundaries; ensure claimed
  durability and applied positions match recoverable data.
- RYW reads before and after catch-up, timeout, snapshot replacement, and epoch
  change; no stale success-shaped fallback.

Measure client RPS and tail latency, CPU cost, bytes copied per mutation, snapshot
pause duration, journal occupancy, recovered gaps, replica lag, and RYW wait time.
Record workload, backend, payload sizes, and acknowledgment policy with results.

## 13. Decisions still required

Before implementation, choose journal capacity and overflow policy, daemon
durability, snapshot-view mechanism, effect/wire encoding, expiration semantics,
probe ABI, endpoint authentication, and RYW client/proxy integration.

These are explicit design choices, not guarantees implied by this proposal.
