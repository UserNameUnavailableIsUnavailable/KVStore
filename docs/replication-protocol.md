# Replication procedure (formal)

This is the procedure as implemented. It is the *push* model: the master copies each
command it executes to every registered replica; a replica never names an offset to
resume from. Redis PSYNC/REPLCONF are deliberately not followed.

## 1. Terms

| Symbol | Meaning |
| --- | --- |
| `M` | master |
| `B` | broker (a `/dev/null` replica that applies nothing and blindly relays) |
| `S` | replica (attaches to `B` as its slave, or to `M` directly) |
| `epoch` | `M`'s replication id, fixed for one master process (`aof_.epoch()`) |
| `o` | a write offset: the master's write counter, incremented once per applied write, starting at 1 |
| `head` | `M`'s last applied offset (0 before the first write) |
| `backlog` | a bounded window of the most recent `(o, payload)` records on `M` |
| `n` | the snapshot offset: `head` sampled at the instant the RDB snapshot was forked |
| `a` | the `head` the peer reports in `+CONTINUE` (`M` only — `B` sends no greeting) |

State kept:

- `M`: `epoch`, `head`, `backlog` (window `[first+1 .. head]`). **No per-replica cursor.**
- `S`: the last offset it has applied, `applied`; a buffer of received, not-yet-applied records.
- `B`: one upstream socket and, at most, one attached slave socket. **No offsets, no
  epoch, no queues** — it never parses the stream.

## 2. Wire format

All control messages are RESP arrays of bulk strings (a bare line is not accepted).
Data frames are a header line followed by exactly `length` raw bytes.

| Message | Direction | Meaning |
| --- | --- | --- |
| `REPLICA` | `S`/`B` → `M` | register this connection for pushed commands (no arguments) |
| `+CONTINUE <epoch> <head>` | `M` → `S`/`B` | registration accepted; the push starts after `head` |
| `@<epoch> <o> <len>\r\n<payload>` | `M` → `S`/`B` | one executed command, `len` bytes of RESP follow |
| `@<epoch> <o> 0\r\n` | `M` → `S`/`B` | idle heartbeat (no payload); `o` equals the last offset sent |
| `SYNC` | `S` → `M` | request a snapshot |
| `+FULLSYNC <bytes> <ip>:<port> <n> <epoch>` | `M` → `S` | snapshot size, RDMA endpoint, snapshot offset, epoch |
| *(binary)* | `M` → `S` | `bytes` of `dump.rdb` over RDMA |

`REPLICA` and `SYNC` are terminal for their connection.

A slave of `B` sends nothing at all: `B` consumes the single `+CONTINUE` it gets
from `M` and never emits one, so a `B` connection is pure downstream bytes. That is
why a slave attaching to `B` must bootstrap its epoch and offset from the first frame
it can parse (§4).

## 3. Master procedures

### 3.1 `record(payload)` — the copy step

When a write is applied, the master increments its counter to `o` and:

```
backlog.append(o, payload)          # evict oldest until within capacity
notify(all push loops)
```

The master logs every write from startup (the inline fast path is disabled while
replication is active), so the backlog is one contiguous run from offset 1. A
follow-up gap in offsets is impossible by construction; if one ever occurred the
window would be dropped and every replica would be told to resynchronise.

### 3.2 `REPLICA session` — the push loop

```
cursor := head                                  # start at "now"; S did not choose this
send "+CONTINUE <epoch> <head>"
loop:
    while record := backlog.next(cursor):        # next(cursor) = (cursor+1, payload)
        send "@<epoch> <cursor+1> <len>\r\n" ; send payload
        cursor := cursor+1
    if backlog does not still contain cursor:    # this replica fell out of the window
        close(session)                            # it must resynchronise
    else:
        wait until head != cursor, or 1s
        if still idle and head == cursor:
            send "@<epoch> <cursor> 0\r\n"        # heartbeat
```

Consequences:

- A registered replica receives commands in exactly the order the master executed them.
- The master holds nothing per replica except the socket and `cursor`.
- A replica that cannot drain before its `cursor` scrolls out of the backlog is dropped.

### 3.3 `SYNC session` — the snapshot

```
n := head                                          # sampled synchronously, no write in between
fork a child that writes dump.rdb                  # the snapshot is the store at n
open an RDMA listener on an ephemeral port
send "+FULLSYNC <bytes> <ip>:<port> <n> <epoch>"
accept one RDMA connection; send the mmap'd dump; close
```

`n` is sampled *before* the fork and no write interleaves with the fork, so the dump
and `n` agree: the dump contains exactly the writes `1..n`.

## 4. Replica procedure

The stream comes from `B` when `--broker` is set, otherwise from `M`. Both are the
same shape; call the chosen endpoint `E`. They differ in one respect: `M` speaks the
`REPLICA` handshake and tells the replica where the stream starts, while `B`
forwards blindly and says nothing, so the replica must find its own start.

```
# 1. register first, so the buffer covers the snapshot window
connect E
if E == M:  send REPLICA ; read "+CONTINUE <E.epoch> <a>"    # a = M's head
else:       nothing                                         # a is unknown
spawn reader: append each @-frame to buffer (drop the connection on overflow)

# 2. take a snapshot
connect M ; send SYNC
read "+FULLSYNC <bytes> <ip>:<port> <n> <m.epoch>"
receive dump over RDMA ; write it ; restore the store
close the connection to M                          # no longer needed

# 3. hand off
require E.epoch == m.epoch                         # else discard the buffer and goto 1
  # via a broker, E.epoch is not known until its first frame arrives: the reader
  # skips bytes until a line parses as @<epoch> <o> <len>, adopts that epoch and
  # offset, and the epoch is checked against m.epoch when the first record is
  # applied. A false start cannot corrupt state: it either matches m.epoch and
  # continues, or fails the gap test below and forces a resync.
discard every buffered record with offset <= n
apply buffered/live records n+1, n+2, ... in order
```

`applied` starts at `n`. Each applied record must be exactly `applied+1`; a gap, an
epoch mismatch, or a dead stream means the buffer is unusable: discard it and
repeat from step 1 (a full resync).

### Why `S` has everything after `n`

`E.head` only increases, and registration happens before the `SYNC` that samples `n`:

```
a = E.head at registration   <=   n = E.head at SYNC   (both sample the same counter)
```

`S` buffered every record `E` pushed after `a`, i.e. `a+1, a+2, ...`. Since
`a <= n`, that buffered set contains every record `n+1, n+2, ...`. Discarding `<= n`
and applying `> n` therefore continues exactly after the snapshot. This is the only
property the handoff needs; it does not depend on the specific value of `a`.

## 5. Broker procedure

`B` is a replica to `M` and a blind pipe to its subscribers. It applies nothing and
interprets nothing: the bytes `M` pushes are handed to whichever slave is attached.

```
# upstream — the only handshake in the system
connect M ; send REPLICA
read "+CONTINUE <epoch> <head>"      # consumed and discarded, never forwarded
loop: read bytes from M
      forward them verbatim to the attached slave (if any)
      on EOF/error: drop the slave, reconnect, repeat

# subscriber (on accept, no handshake)
attach the new socket as the slave; the previous slave (if any) is dropped
```

Forwarding is done in the kernel: the upstream socket goes into the sockhash
`upstreams` and the slave into the sockmap `slaves`, with an `sk_skb/stream_verdict`
program attached to `upstreams` alone that returns
`bpf_sk_redirect_map(skb, &slaves, 0, 0)`. Ingress from `M` is therefore spliced to
the slave with no userspace copy and no parsing. Attaching the programs to
`upstreams` only keeps the redirect one-directional — a slave's own ingress is never
verdict-processed, so it can never be redirected into itself. With no slave attached
the redirect fails and the data is dropped, which is the `/dev/null` behaviour of a
broker with no replica.

The two maps are deliberately of different types. The destination must be a
`SOCKMAP`, not a `SOCKHASH`: the verifier lets only `bpf_sk_redirect_hash` touch a
`SOCKHASH`, and passing one to `bpf_sk_redirect_map` fails the program load with
`-EINVAL` ("cannot pass map_type 15 into func bpf_sk_redirect_map"). A single socket
slots into an array index anyway.

Where BPF cannot be installed (no `CAP_BPF`/`CAP_SYS_ADMIN`, no object) the broker
falls back to a userspace byte relay with the same blind semantics: `read` the
upstream and `write` the same bytes to every attached slave, dropping any slave
whose queue exceeds 16 MiB.

Because there is no per-subscriber greeting, a slave attaches mid-stream and may
receive a partial frame. It skips input until a line parses as a frame header (§4),
so an attach at any moment is acceptable: the replay either continues from the
slave's snapshot offset or fails the gap test and forces a resync. This is what
satisfies "a slave may attach at any time".

## 6. Invariants

1. **Epoch constancy** — within one master process `epoch` never changes; a replica
   rejects any frame whose epoch differs from its snapshot's.
2. **Contiguity** — a data frame's offset is always `previous + 1` on that connection;
   a heartbeat carries no offset advance.
3. **Snapshot agreement** — the dump equals the store at `n`, because `n` is sampled
   with no write between the sample and the fork.
4. **Coverage** — the registration (or, via a broker, the attach) precedes the snapshot
   sample, so everything pushed after it contains `n+1, ...`. The one frame a broker
   may cut in half at attach time began before the attach, so its offset is `<= n` and
   the dump already contains it.
5. **Order preservation** — the master appends to the backlog inside `record()`, in
   execution order and without suspension, so the pushed order is the applied order.

## 7. Failure handling

| Event | Detected by | Action |
| --- | --- | --- |
| Replica leaves / TCP reset | `send` fails | drop the socket |
| Replica too slow | `cursor` scrolls out of backlog | master closes it; replica resyncs |
| Replica's buffer overflows while syncing | reader | drop stream; resync |
| Gap in the pushed stream | `o != applied+1` | discard buffer; resync |
| Epoch differs from snapshot | compare on handoff | discard buffer; re-register |
| Snapshot restore fails | checksum/RDB load | retry `SYNC` |
| Broker loses upstream | `recv` returns 0 / non-contiguous frame | drop all subscribers; reconnect and re-`REPLICA` |

There are no ACKs and no per-replica offsets on the master; recovery is always "take a
new snapshot", never "resume from an offset the replica asks for".

## 8. Worked example (broker path)

```
M: head=0
B: REPLICA            ; M answers +CONTINUE 7 0, which B discards
M executes SET foo bar -> o=1 ; pushes @7 1 ... to B ; B relays to S
M executes SET k1 v1  -> o=2 ; pushes @7 2 ...       ; relayed to S
S: (connect B)        ; no handshake; the first bytes S sees are @7 1 ...
S: SYNC -> M
M: +FULLSYNC 96 <ip>:<port> 2 7        ; n=2
S receives dump (state has foo,k1) ; applied := 2 ; learns m.epoch = 7
S discards buffered 1..2 (epoch 7 == m.epoch) ; next expected offset is 3
M executes SET k2 v2 -> o=3 ; @7 3 ... reaches S via B ; S applies (3 == applied+1)
```

Result on `S`: `foo`, `k1` from the dump; `k2` from the stream; no re-sync.

## 9. Mapping to the draft in `replication.md`

| Draft | Implemented | Note |
| --- | --- | --- |
| `REPLICAOF <ip> <port>` | `SLAVEOF <ip> <port>` (+ `--broker`) | command name only |
| `RSYNC <replication_id>` | `REPLICA` (no argument) | registration carries no id/offset |
| `FULLSYNC <offset>` | `+FULLSYNC <bytes> <ip>:<port> <n> <epoch>` | endpoint and epoch ride along |
| `SYNCMODE EXPERIMENTAL/CLASSIC`, `SYNCADDR` | *(none)* | the endpoint is always RDMA, in `+FULLSYNC` |
| `REALTIME <offset> <command>` | `@<epoch> <o> <len>\r\n<payload>` | same idea, epoch-tagged framing |
| S replays `n+1 .. m-1`, then closes M | S discards `<= n`, applies `> n` from the buffer | the buffer starts at `a <= n`, so `n+1..` is already there; there is no `n+1..m-1` gap to fill from `M` |

The one substantive difference to reconcile: in this implementation the replica does
**not** keep `M` open to fill a middle range, and the broker stream is not "everything
after the dump". The broker stream starts wherever the attach lands, which is **at or
before** `n`, so the replica already holds `n+1, ...` in its buffer and the middle
range never has to be requested from `M`.
