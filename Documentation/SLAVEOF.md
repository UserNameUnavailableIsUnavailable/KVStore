# `SLAVEOF`

The `SLAVEOF` command makes an instance replica of a master instance.

Prototype: `SLAVEOF <address> <port>`

`SLAVEOF` connects to the master, requests `PSYNC`, and applies the returned
command stream locally. A full synchronization replaces the local store before
the stream is replayed. The replica remembers the master's replication id and
offset, so a later `SLAVEOF` request to the same master can receive a partial
synchronization.

The address must be an IPv4 address and the port must be in the range 1-65535.