# Replica

## Overview

### `SLAVEOF`

The `SLAVEOF` command makes an instance replica of a master instance.

Prototype: `SLAVEOF <address> <port>`

### `PSYNC`

`SLAVEOF` connects to the master, requests `PSYNC`, and applies the returned
command stream locally. A full synchronization replaces the local store before
the stream is replayed. The replica remembers the master's replication id and
offset, so a later `SLAVEOF` request to the same master can receive a partial
synchronization.

The address must be an IPv4 address and the port must be in the range 1-65535.
`PSYNC` triggers synchronization from a master to a replica. Replicas issue it
implicitly through `SLAVEOF <address> <port>`. The master responds with its
replication id and the latest command offset.

The replica uses this id and offset on a later `PSYNC` request to continue from
the command it already applied.

Prototype: `PSYNC <replica-id> <offset>`

The first synchronization is full. The master returns a transactional command
stream (`MULTI`, `SET`/`EXPIRE`, `EXEC`) which replaces the replica's local
store. The master also retains the latest 256 successful `SET`, `DELETE`, and
`EXPIRE` commands in memory. A known replication id with an offset still within
that backlog receives a partial stream containing only newer commands; an
unknown id or an expired offset receives a full stream.

## Implementation Details

### State Transition

Once the connection between the master and the slave is established, the conncetion persists.

The replica owns a persistent master `TcpSessionService` for the lifetime of the
replication link. The master keeps a persistent `TcpSessionService` for every registered
replica, independently of the ordinary client session that received the initial
`PSYNC` request. TcpSocket descriptors remain inside each `TcpSessionService` and are used
only where operating-system I/O requires them.

1. The slave initiates a connection with the master by `SLAVEOF`.
2. The master assigns an id for the slave, creating a full snapshot for the slave.
3. The slave receives and replays the snapshot.
4. The slave constantly sends heart beat requests to the master.
5. The master could decide whether to respond with recent executed commands to the client. Specifically, the master checks the fullness of the buffer and the last synchronization timepoint. If the master detects that the buffer is gonna overflow (e.g., 75% of the capacity), or 60s since last synchronization (this means data updates is not very frequent), then the master will push incremental updates anyway. One edge case is that no update happens in the last 60s, then the master keeps silent.

### Response

The RESP response is an array with four elements:

1. `FULLRESYNC` or `CONTINUE`
2. Master replication id
3. Latest master offset
4. A bulk string containing RESP-encoded commands to apply