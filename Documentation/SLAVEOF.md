# `SLAVEOF`

The `SLAVEOF` command makes an instance replica of a master instance.

Prototype: `SLAVEOF <address> <port>`

It is the interface a *client* uses, and the only command that changes what an
instance is. From the moment it is accepted the instance is a replica and
read-only: writes from its own clients are refused with
`-READONLY You can't write against a read only replica.`, whether or not the link
to the master has come up yet. A store that is about to be replaced by a master's
does not belong to this instance's clients any more.

`<address>` is the master's **RDMA** address — the one it was given with
`--replication-ip` — not the TCP address it answers clients on, and `<port>` is
its `--replication-port`. The address must be an IPv4 address and the port must
be in the range 1-65535.

What the two servers then say to each other is `PSYNC`, which no client sends;
see `Documentation/PSYNC.md` for that exchange and
`Documentation/REPLICATION.md` for the whole path.

`SLAVEOF` cannot be run twice on the same instance. A second one is refused
rather than moving the instance to another master: what is being asked for there
is a different operation, and it would have to replace this instance's store the
way the first one did.

An instance started without `--rdma-device` has no link to follow a master on, and
`SLAVEOF` is refused with that as the reason rather than a link that never comes
up.
