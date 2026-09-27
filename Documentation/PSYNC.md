# `PSYNC`

`PSYNC` is the command the two servers speak to each other over the replication
link. It is not part of the client interface: a client asks for replication with
`SLAVEOF <ip> <port>`, and the instance it was sent to is what connects and sends
`PSYNC`. See `Documentation/SLAVEOF.md`.

The link is RESP over `RdmaDeliverService`, so the requests are ordinary commands
— an array of bulk strings — and the replies are ordinary replies, with one
exception noted below. The whole exchange, in order:

```
replica -> master   *3 $5 PSYNC $1 ? $2 -1
master  -> replica  +FULLRESYNC <replid> <offset>
master  -> replica  $<length>
master  -> replica  <length bytes of RDB, raw>
replica -> master   *3 $5 PSYNC $<n> <replid> $1 0
master  -> replica  +CONTINUE <replid>
master  -> replica  <the writes, as they are applied>
```

**The first request** carries `?` and `-1`: no replication id, and an offset
before anything. It is the only thing a replica can say before it has seen the
master's stream, and it can only be answered with a snapshot.

**`+FULLRESYNC <replid> <offset>`** says which stream this is and where in it the
image that follows was taken. The offset is the master's write counter at the
instant `BGSAVE` copied the store, so it is the position a replica is caught up
from.

**`$<length>`** is a bulk string's header, and the RDB that follows is the
`<length>` bytes themselves. It is **not** terminated by CRLF, unlike every other
bulk string: the length is what says where the image ends, and a terminator would
be one more thing to tell apart from the first byte of the next command.

**The second request** is sent once the image has arrived, validated against its
own CRC-64 and loaded. It names the replication id it was just given, which is
what says it loaded *this* master's image, and an offset of `0` — a placeholder,
because this connection's buffer was begun at the snapshot and the master already
knows where that is. A replica that quotes an id this master does not recognise
is a replica that loaded an image from somewhere else, and the link ends rather
than continuing from a stream it never had.

**`+CONTINUE <replid>`** is the master saying the stream starts here. Nothing is
asked for after it: the master sends each write as it applies it, and the replica
applies them in the order they arrive.

## What each end has to hold

The master attaches the replica to its write path at the instant the snapshot is
captured, so the buffer holds everything applied since — including what is applied
while the image is still on the wire. Nothing is lost between the snapshot and
the stream, and nothing is sent twice.

On the replica the stream is a byte stream, not a sequence of commands: a payload
ends wherever the master cut it, which is as likely to be the middle of a command
as between two. The decoder is therefore kept across payloads, and what it has
taken out of the buffer is only taken once a whole command is there.
