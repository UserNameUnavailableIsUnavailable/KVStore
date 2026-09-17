# Replication

Replication is a pluggable service of the server, in `Application/Server`.
The server owns the store; the service owns the replication port and everything
that crosses it. The wire is RDMA, not TCP: a replica is handed the master's
snapshot as a *file*, in packets, over a reliable connection.

The service is described by two things a caller gives it:

- `ReplicationService::Options` — whether to listen (`--replication-port`), the
  RDMA address to bind, and the master to follow (`--replicaof`).
- `ReplicationService::Host` — the callbacks back into the server: where the
  snapshot file lives, how to take one, how to load one, and what to do when the
  link comes up or goes down.

## Command line

```
Server --port 8080 \
       --replication-port 9000 --replication-address 192.168.0.201
Server --port 8081 --replicaof 192.168.0.201:9000
```

- `--port` — the TCP port clients connect to.
- `--replication-port` — the RDMA port this server serves snapshots on. `0`
  (the default) serves none.
- `--replication-address` — the local address the RDMA listener binds. It has to
  name the RDMA device (`siw0` is `192.168.0.201` on the development machine): a
  wildcard such as `0.0.0.0` is accepted by `rdma_bind_addr` but binds no device,
  so a replication port without an address that names one is refused at startup
  rather than served from nowhere.
- `--replicaof <ip>:<port>` — the RDMA address of a master to replicate. A
  replica is **read-only**: writes are refused with
  `-READONLY You can't write against a read only replica.`
- `--config <file>` — a startup command file, one command per line, run before
  the server accepts a client. The port it serves on and the address it serves
  replicas from are named in it as `CONFIG` commands — `config port 8080`,
  `config replication_address <ip> <port>` — and read while the server is built,
  because neither can be changed once it is listening. See
  `Documentation/CONFIG.md`.
- `KVSTORE_LOG_LEVEL=debug` raises the log level; the transfer logs its packets
  and credits at `debug`.

## Full synchronization

1. The replica connects and **opens the transfer**, offering what it can take:
   its own chunk size and how many packets it can hold at once. Nothing is sent
   by the master before that: the receiver is the one that knows it has receives
   posted.
2. The master takes a snapshot — a `BGSAVE`, so it forks and the child writes
   the RDB — and copies the store *before* it yields, so the cursor that follows
   the write log is taken at exactly the moment the snapshot represents.
3. The master decides what it will actually send — the smaller of the two chunk
   sizes, and a window it can have outstanding — and says so in one message.
4. The master sends the RDB file, cut at that size, one packet per message, and
   never more packets than the window while the replica has not written them
   out. The replica reports every packet as it writes it, and the master sends
   the next one against that report.
5. The replica receives it into `dump.rdb.part`, and the file is moved onto
   `dump.rdb` only once every packet has arrived. Validating and replaying are
   the same step: an RDB ends with a CRC-64 over its own bytes, and loading
   refuses an image whose checksum does not match. A transfer that lost or
   damaged a byte therefore cannot become the store.
6. The replica confirms the number of bytes it wrote, and the master reports the
   transfer complete. The link stays up afterwards: that is what the incremental
   stream will be pushed over.

`BGSAVE` is the only snapshot command; it forks a child, which is why it is not
called `SAVE`.

Because the transfer is a file and the store is only replaced after the file
validates, an interrupted full sync leaves the replica exactly as it was.

### Nested replication

A replica is still a server: give it a `--replication-port` and another server
can replicate *it*. The replica serves its own snapshot from its own store, so
nesting needs no special case.

## The transfer protocol

Moving a payload over an RDMA session is `Application/Server/RDMA_Transfer`.
Four messages, each starting with its operation as a NUL-terminated word, and
all numbers in network byte order — big endian — as the snapshot's own lengths
are, so the high byte of a count is where a reader looks for it rather than
where the machine it landed on would put it:

```
receiver -> sender : "<kind>\0" chunk_size:u32 num_chunks:u32
sender  -> receiver: "PLAN\0" chunk_size:u32 packets:u32
sender  -> receiver: the payload, one packet per message
receiver -> sender : "CREDIT\0" written:u32    after each packet but the last
receiver -> sender : "DONE\0" bytes:u64        in place of the last credit
```

The word is what tells the messages apart, so a stray message is refused rather
than read as the wrong kind of number — and the opening's word is the payload's
**kind**, which is the one thing the framing cannot say for itself:

The counts on the wire are the widths they need and no more: the packet count is
32 bits because a credit carries the count written out in the same width, so a
payload whose packets did not fit it could be announced and then never reported —
the sender refuses such a payload instead. The byte total in the confirmation is
64 bits, because that one is the payload's size.

| kind | what the bytes are | what the receiver does with them |
| --- | --- | --- |
| `RDB` | an opaque payload | writes it out whole, and validates it before it becomes the store |
| `RESP` | RESP-encoded commands | decodes them as they arrive and applies them |

The transfer never looks inside either, so the kind stands in for looking: the
receiver knows which of the two it is holding, and a sender that has the other
one is refused rather than allowed to put bytes on the wire that the far end has
already said it cannot use. The two words are different lengths, so an opening is
one of two sizes and never something in between.

Every packet is reported exactly once: a credit for each packet the receiver
writes, and the confirmation in place of the credit for the last one. The two
would otherwise be the same message twice — a credit for the last packet already
says every packet is written — so the confirmation carries the byte total
instead, and it is the one message that says the payload arrived and not merely
left.

The payload is whatever the sender can hand out a packet at a time — a file, a
buffer in memory, or nothing at all — and how to slice it is the application's
decision, which is why this lives in `Application` and not in the transport.

### The negotiation

The receiver opens, offering what *it* can take:

| field | meaning |
| --- | --- |
| kind | the word that says what the payload is: `RDB` or `RESP` |
| `chunk_size` | the largest packet it will accept — the size of the buffers it posted |
| `num_chunks` | how many packets it can hold at once — how many receives it posted |

The sender then decides, because the two ends need not be configured the same and
both limits are hard:

```
chunk_size = min(the offer, the sender's own chunk size)
window     = min(the offer's num_chunks, the sender's posted receives)
```

Sending more than the offer is not refused by the device, it is *truncated*: too
large a message does not fit the receive it lands in and the excess is dropped,
so the packet size has to be respected rather than discovered. And this is not a
formality for the mirror case either — the two servers of a test are configured
apart from each other, so the smaller chunk size wins and the same file arrives
intact through 1024-byte packets as through 4096-byte ones.

Neither half of the window minimum is about the payload alone: reports come back
the other way too, and they land in the *sender's* receive queue. What makes the
two counts the same number is that the reports mirror the packets one for one, so

```
unread packets in the receiver + unread reports in the sender
    = packets received - reports read
    <= packets sent - credits read
    <= window
```

and a single window bounds what the two queues hold between them, however the
traffic divides. So nothing is held back for the confirmation — it is not an
extra message, it is the report for the last packet — and it is just as much a
limit as the payload is: a report that arrives with no receive posted is what
ends a connection on iWARP. That is the same sizing argument as
`kReceiveChunks > kSendChunks`, one level up.

### Why the reports exist

RC delivery is reliable, but a *send completion only says the bytes left*: it
says nothing about whether the peer had a receive to put them in, and on iWARP a
message that arrives with no receive posted ends the connection — there is no
RNR to fall back on.

So a sender that refills its window from its own completions can outrun a peer
that has not had a chance to reap and repost. That is not hypothetical: with a
16-deep receive queue, an 18-packet snapshot failed at packet 17, because the
whole burst arrived before the receiver's event loop ran once.

The window is therefore measured in what the receiver has *written out*, never in
what the sender has handed to the device:

- `written` is the highest count the receiver has reported, 0 before the first
  credit. It is cumulative rather than "one more", so the two ends cannot drift
  apart by counting differently, and the sender can see exactly how far the
  payload has really got. It is always short of the packet total, because the
  report for the last packet is the confirmation.
- `sent` is how many packets the sender has handed to the device.
- The sender may send another packet only while `sent < written + window`, which
  in its own bookkeeping is
  `in flight <= min(num_chunks the receiver posted, credits it has left)`, the
  credits starting at the window with one spent per packet.
- The receiver releases the buffer *before* it sends the report, so a credit is
  the proof that the buffer is back: there is never a moment where the sender is
  entitled to a slot the receiver has not reposted.
- The sender's own chunk pool bounds it further, because it cannot post more
  sends than it has chunks: the pool is what is binding when it is the smaller of
  the two. It bounds the packets on the wire, not the span they cover — a send
  that has completed frees its chunk while its credit is still unread.
- A credit may not name more packets than there are, nor the whole payload, and
  the confirmation may not arrive before the sender has sent everything: each of
  those would mean the two ends were counting different transfers.

A credit can count to 2³²-1 packets, so a payload longer than that is refused at
both ends rather than reported wrongly.

## The write log

While a replica is attached, the master records every write it applies -- the
commands, RESP-encoded once, in the order they were applied -- into one
`WriteHistory` that all the replicas read from. Recording starts at the instant
the snapshot copied the store: `serve_replica` asks for the snapshot and attaches
a cursor with nothing awaited in between, so a write is either in the image or
after the cursor, never in both. `ReplicationService::record` is called from the
command path for exactly the commands `IsWriteCommand` accepts, which are also
the ones the AOF records, and it is called *before* the AOF append is awaited for
the same reason.

The log is a run of 64 KiB blocks. Each block carries a reference count: one for
every cursor that was behind it when it was written, given back when that cursor
reads past it. A cursor is one reader's position and the only state a replica
needs -- it is where the replica is caught up from, and the offsets go on across
a link that goes away and comes back, so a replica that reconnects says where it
had got to and is answered from there. The head of the log leaves once no cursor
holds it, so what is kept is what the replicas still need, and a reader that is
done never has to tell the log anything.

A log that no replica can read cannot be held open forever. Past the capacity
the head goes anyway -- 4 MiB by default, which has to cover a snapshot transfer
at the write rate of the master -- and every cursor that still needed those bytes
is marked invalid rather than handed a stream with a hole in it. The replica is
told (`valid()` is false), what it lost is counted in `dropped_bytes()`, and it
synchronises from scratch.

The reference counts are the one piece of state that can drift without anyone
noticing: too high and the log never shrinks, too low and bytes are dropped while
a reader is still owed them. `WriteHistory::validate()` recomputes them from the
cursors and is asserted after every step of a randomized run -- which is the
whole reason to keep the counts rather than derive them at each decision.

Cutting the log at a replica's cursor and pushing it over the link after a
Cutting the log at a replica's cursor is the incremental stream, and it is what
the link is kept for. It runs one batch per request: the replica asks from the
offset it has applied, the master answers with what the log holds from there, and
the offset in the request is both the replica's position and its acknowledgement
that everything below it is done with -- which is what lets those blocks go. When
the log holds nothing past the offset the request waits on the write history's
condition variable rather than an empty batch going out and coming straight back,
so an idle replica costs a parked coroutine and nothing else.

The offset travels with both halves of the sync. The snapshot's opening is a
plain `RDB` transfer, but its *plan* says which log position the image was taken
at, which is where the replica follows from; a request for commands says where to
start, and the plan that answers it says where the run ends. The replica counts
what it applied with the bytes each command encodes to -- the same bytes the
master's log holds -- so the two ends count the same thing and a batch that ends
in the middle of a command is simply asked for again.

Applying what arrives is the server's, not the service's: `Host::apply` mutates
the store with the same commands the AOF replay knows, records the write in this
replica's own log so a replica of this replica is caught up too, and does it
without the read-only check -- that check is for clients, and a replica is
read-only to them, not to its master.

A replica whose offset the log no longer holds -- it fell off the far end while
the link was down -- is told so by the link ending, and synchronises again from a
snapshot.

## Limitations

- A replica's AOF, if enabled, is not rewritten when a snapshot replaces its
  store, so replaying it after a restart applies commands against an image they
  were not written for. Enable the AOF on masters, not on replicas, for now.
- A master's stream for a replica that goes away while it is waiting for a write
  stays parked until the next write wakes it and the send fails. It holds that
  replica's blocks until then.
- Replication is not identified: a replica that is pointed at a different master
  with the same offset numbers would be served from the wrong log. There is no
  replication id in the protocol yet.
- `Documentation/PSYNC.md` and `Documentation/SLAVEOF.md` describe an earlier
  TCP-based design for the incremental half; nothing in them is implemented in
  this path.
