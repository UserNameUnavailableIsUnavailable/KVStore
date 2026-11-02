# `SYNC`

`SYNC` is the command the two servers speak to each other over TCP. It is not
part of the client interface: a client asks for replication with
`SLAVEOF <ip> <port>`, and the instance it was sent to is what connects and sends
`SYNC`. See `Documentation/SLAVEOF.md`.

The exchange is RESP over TCP, with the dump carried on a separate RDMA link:

```
replica -> master (TCP)   *1 $4 SYNC
master  -> replica (TCP)  +FULLSYNC <bytes> <rdma_ip> <rdma_port>
master  -> replica (RDMA) <bytes of RDB, raw, mmapped and sent whole>
```

With a write-log daemon, one more line precedes `+FULLSYNC`:

```
master  -> replica (TCP)  +REPLICATION <replid> <offset> <repld_endpoint> <ticket>
```

**`+FULLSYNC <bytes> <rdma_ip> <rdma_port>`** says how large the dump is and where
it will arrive: the replica opens a fresh RDMA connection to that endpoint, posts
one receive of `<bytes>`, and writes what arrives to disk. The dump is the whole
RDB file, CRC-64 checked before it replaces the store.

**`+REPLICATION ...`** names the stream the replica should chase afterwards: the
replication id, the write-counter offset the snapshot was taken at, the repld
TCP endpoint, and the handoff ticket that authorises the repld connection.

## What each end has to hold

The master forks the snapshot and attaches the replica to the write log at the
instant it is captured, so repld's backlog holds everything applied since. The
dump travels on RDMA, and the incremental stream follows on repld over TCP: the
two move in parallel, and the replica applies the backlog only after the dump has
been restored.
