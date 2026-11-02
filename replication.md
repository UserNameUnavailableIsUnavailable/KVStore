# Replication

Given master $M$, broker $B$, and slave $S$, the replication process can be formalized as follows:

## Direct Replication

1. $S$ connects to $M$, then $M$ prepares `dump.rdb` for $S$, responding with the dump file and offset $n$.
2. $S$ loads the dump file, then starts receiving and applying commands from $M$, starting from offset $n + 1$.

## Replication with Broker

1. $B$ connects to $M$ at the very beginning. It behaves as a normal slave, except that it does not actually apply the commands it receives from $M$.
2. $S$ connects to $B$, then $B$ starts forwarding the master-slave traffic to $S$, and $S$ buffers the received commands (it ignores incomplete commands). The first complete command's offset is denoted $m$.
3. $S$ connects to $M$, then $M$ prepares `dump.rdb` for $S$, responding with the dump file and offset $n$.
4. $S$ loads the dump file, then replays commands from $n + 1$ to $m - 1$ (pushed by $M$), then $S$ closes the connection with $M$.

## Commands

### `REPLICAOF <ip> <port>`

This is a user command. It instructs the current node to become a replica of the specified master `<ip>:<port>`.

### `RSYNC <replication_id>`

Initiates the replication process by requesting the master or broker to start sending commands from the specified replication ID.

#### Between Master and Slave

- S: `RSYNC <replication_id>`.
- M: `FULLSYNC <offset>`. `offset` is the index of last write applied by the master.
- S: `SYNCMODE <EXPERIMENTAL|CLASSIC>`.
- M: `SYNCADDR <ip> <port>` (RDMA for `EXPERIMENTAL` mode; TCP for `CLASSIC` mode).
- S connects to the master at the specified `<ip>:<port>`.
- M sends the `dump.rdb` and closes the connection.
- M: `REALTIME <offset> <command>`.
- S: closes the connection with the master at some point after receiving several `REALTIME` commands.

#### Between Broker and Slave

- S: `RSYNC <replication_id>`.
- B: `REALTIME <offset> <command>` (forwarded by daemon).