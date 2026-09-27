# Replication

Replication is a pluggable service of the server, in `Application/Server`.
The server owns the store; the service owns the replication port and everything
that crosses it. The wire is RDMA, not TCP, and what crosses it is **RESP** —
the protocol a client speaks — over `RdmaDeliverService`.

The service is described by two things a caller gives it:

- `ReplicationService::Options` — whether to listen (`--replication-port`) and
  the RDMA address to bind.
- `ReplicationService::Host` — the callbacks back into the server: where the
  snapshot file lives, how to take one, how to load one, how to apply a command
  the master applied, and what to do when the link comes up or goes down.

## Command line

```
Server --port 8080 --replication-port 9000 --replication-ip 192.168.0.201 --rdma-device siw0
Server --port 8081 --rdma-device siw0

redis-cli -p 8081 SLAVEOF 192.168.0.201 9000
```

- `--port` — the TCP port clients connect to.
- `--replication-port` — the RDMA port this server serves replicas on. `0`
  (the default) serves none.
- `--replication-ip` — the local address the RDMA listener binds. It has to
  name the RDMA device (`siw0` is `192.168.0.201` on the development machine): a
  wildcard such as `0.0.0.0` is accepted by `rdma_bind_addr` but binds no device,
  so a replication port without an address that names one is refused at startup
  rather than served from nowhere.
- `--rdma-device <name>` — the device the link runs on. Serving replicas needs
  it; so does following a master, which `SLAVEOF` asks for after startup.
- `--config <file>` — a startup command file, one command per line, run before
  the server accepts a client. The port it serves on, the address it serves
  replicas from, and the device are named in it as `CONFIG` commands and read
  while the server is built, because none of them can change once it is
  listening. See `Documentation/CONFIG.md`.
- `KVSTORE_LOG_LEVEL=debug` raises the log level.

Which master this instance follows is **not** a startup setting. `SLAVEOF <ip>
<port>` names one, and a client sends it once the server is answering; from that
moment the instance is a replica and **read-only** — writes are refused with
`-READONLY You can't write against a read only replica.` The address is the
master's *RDMA* address, not the TCP port it answers clients on. A second
`SLAVEOF` on an instance that already follows a master is refused rather than
silently moving it.

## Full synchronization

1. The replica opens the link and sends `PSYNC ? -1`: no replication id, and an
   offset of `-1`, which is the only thing it can honestly say before it has seen
   the master's stream.
2. The master takes a snapshot. `Host::snapshot` is a `BGSAVE`: it **copies the
   store before it returns** and hands back a task that forks the child which
   writes the RDB. The replica's buffer is attached at the instant that copy is
   taken, so a write lands either in the image or in the buffer that follows it —
   never in both, never in neither.
3. The master replies with the position the image was taken at, and the length of
   the image:

   ```
   +FULLRESYNC <replid> <offset>\r\n
   $<length>\r\n
   <length bytes of RDB>
   ```

   The RDB is sent raw: its length is what says where it ends, so there is no
   terminator to confuse with the first byte of the next command.
4. The replica writes those bytes to `dump.rdb.incoming` and moves the file onto
   `dump.rdb` only once every byte has arrived, so an interrupted transfer leaves
   nothing that looks like a snapshot. Validating and loading are the same step:
   an RDB ends with a CRC-64 over its own bytes, and a load refuses an image whose
   checksum does not match. A transfer that lost or damaged a byte therefore
   cannot become the store.
5. The replica sends `PSYNC <replid> <offset>` — the id it was just given, which
   is what says it loaded *this* master's image. The master answers
   `+CONTINUE <replid>\r\n` and starts the stream.
6. From there the master talks and the replica applies. Every write the master
   applies is encoded once and appended to each replica's buffer, and each
   replica's writer hands that buffer over as fast as the link allows.

`BGSAVE` forks a child, which writes the RDB while the server keeps serving, and
`SAVE` writes the same image in the server's own process, so the server stands
still until it is on disk and answers `+OK` when it is. Neither runs on a
schedule: a snapshot is asked for by name, and `BGSAVE` is the one to ask for
while clients are being served.

Because the store is only replaced after the file validates, an interrupted full
sync leaves the replica exactly as it was.

### The stream, and what buffers it

Each served replica has a `std::string` of encoded writes and a writer coroutine
that drains it. `record()` — called by the server for every write it applies —
encodes the command and appends it to **every** attached replica's buffer, then
wakes the writer if that replica has asked for the stream.

Nothing is dropped when a link is momentarily full. The writer hands the buffer
to `RdmaDeliverService::send()`, which cuts it into packets and waits for the
peer's window; while it waits, `record()` keeps appending. So the buffer is the
difference between what the master has applied and what the link has taken, and
a slow replica makes that difference grow rather than losing writes.

The link is a delivery service rather than raw chunks because a sender that posts
more messages than the peer has receives posted has its queue pair torn down
under it for `RNR_RETRY_EXC_ERR`. Only the delivery layer knows how far the peer
has got, and it is what holds the sender inside that.

On the replica side the stream is decoded with the same RESP decoder the client
path uses, kept across payloads: a payload ends wherever the master cut it, which
is as likely to be the middle of a command as between two.

### Nested replication

A replica is still a server: give it a `--replication-port` and another server
can replicate *it*. The replica serves its own snapshot from its own store — and
records what it applies, so a replica of a replica is caught up from the same
bytes — which is why `apply_replicated_command` calls `record()` too.

## Limitations

- A replica's AOF, if enabled, is not rewritten when a snapshot replaces its
  store. The log is the store when a server with `appendonly yes` restarts, so a
  replica that has taken a snapshot and then restarts comes back to whatever its
  log holds — which predates the snapshot. Enable the AOF on masters, not on
  replicas, until a full sync also rewrites the log.
- A replica's buffer is unbounded. A replica that stays slower than its master
  makes the master hold everything applied since the snapshot.
- The replication id is an integer counter taken at startup, and the offset is a
  per-process counter that begins at 0. Neither is persisted, so a master that
  restarts reuses them, and the offset it reports is not comparable with the one
  it reported before. A replica always synchronises from scratch, so nothing
  depends on them across a restart — but nothing may be built on them either.
- A replica retries the link once a second and logs every attempt, so a master
  that is down for a long time fills the replica's log.
