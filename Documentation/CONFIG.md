# The startup command file

`kvstore-server --config <file>` (short: `-c`) reads a file at startup in which
**each line is a command**, written the way it would be typed at a prompt:

```
# the master's command file
config appendonly yes
set greeting "hello world"
dbsize                      # how many keys the snapshot holds
```

The server runs the file on itself before it accepts a client, through the same
validation and the same execution a client's command goes through. A file cannot
say anything a connection could not, and there is only one set of rules for the
two to drift apart from. Writes are no exception: a `set` in a file goes into the
AOF and out to the replicas exactly as it would from a client.

```
# the shape the settings take: a CONFIG command, one per line
config port 8080
config replication_address 192.168.0.201 8081
config rdma_device siw0
config appendonly yes
```

- Lines split on whitespace. A double-quoted run is one argument, and inside it a
  backslash escapes the next character, so a value with spaces in it can be
  written. Outside quotes a backslash is an ordinary character, which leaves
  values free to look like paths.
- Blank lines and lines whose first character is `#` are ignored, so a file can
  explain itself. There are no comments at the end of a line: everything after
  `#` on a line with a command on it is an argument like any other.
- A line the server cannot carry out **stops the startup**, and the message names
  the file and the line: `master.conf:2: ERR unknown command 'frobnicate'`. A
  configuration is applied or this instance does not come up; it never serves
  clients with half of a file.
- **A few settings are not commands.** These name what the server *is* rather
than something for it to do, so they are read while it is being put together and
taken out of the list instead of run. They are written as the `CONFIG` commands
they are:

  ```
  config port 8080                              # clients connect here
  config replication_address 192.168.0.201 8081 # replicas connect here, over RDMA
  config rdma_device siw0                       # the device that link runs on
  config appendonly yes                         # keep a log of every write
  ```

  `port`, `replication_address` and `rdma_device` have to be known before a socket
  is bound or a channel is opened, and a running server refuses them: there is
  nothing left to rebind. The address has to name the RDMA device — a wildcard such
  as `0.0.0.0` is accepted by `rdma_bind_addr` and names no device at all, so it is
  refused at validation, and again when the service that listens is built.

  `appendonly` is here for a different reason: it decides **which of the two files
  the store is read from**. The log and the image are alternatives, not a sequence —
  the log is the store when this instance was told to keep one and there is one to
  read, and the RDB is what is left for every other case:

  | log kept | `appendonly.aof` | store comes from |
  |---|---|---|
  | yes | present | the log; the RDB is not read |
  | yes | absent | the RDB, and the log is created |
  | no | present | the RDB, with a warning that the log is being ignored |
  | no | absent | the RDB |

  Reading a log on top of an image would apply commands the image already holds,
  and an image newer than the log would have the log's older values put back over
  it; reading only one of them is what avoids both.

  Unlike the other three, `appendonly` is not frozen: a client may still send
  `CONFIG SET appendonly yes|no`, and what that changes is what happens from there
  on. A file has to name it because the choice is made before the server accepts
  anything, not because the choice cannot be changed.

  Everywhere else the rule is one setting, one line. A second line for the same
  setting overrides the first, a flag given on the command line overrides both and
  says so when it does, and a value nobody named falls back to the default (`port`
  8080, `replication_address` unset, which serves no replicas).

  Once the server is serving, these two are refused rather than applied:

  ```
  $ redis-cli -p 8080 CONFIG port 9000
  (error) ERR CONFIG SET failed - 'port' is a startup setting; it can only be named in the command file
  ```

  `CONFIG GET` still answers with what the server decided, so the settings are
  readable even though they are not writable: `CONFIG GET port`,
  `CONFIG GET replication_address` and `CONFIG GET rdma_device` return the values
  this instance was built with.
- Which master an instance follows is **not** a setting, and there is no
  `replicaof` line: a master is named by `SLAVEOF <ip> <port>`, which a client
  sends once the server is answering, and which can therefore be sent to a server
  that is already serving clients. See `Documentation/SLAVEOF.md`.
- `CONFIG` has a short form for exactly this: `config appendonly yes` is
  `CONFIG SET appendonly yes` with the word left out. No parameter is named like a
  subcommand, so the short form takes nothing away from the long one.

## Why the file is commands

A directive language of its own would be a second parser, a second set of names,
and a second place for `CONFIG` to mean something slightly different -- and the
server would still need a way to apply a setting to a store that is already up.
Running commands gives the file everything the command surface has, including
whatever is added to it later, for one validation path and no new state.

The cost is that a file is a *script*: it runs once, in order, at startup. A line
that changes a setting does not describe the instance's state the way a
declarative file would, so the file is the record of what was asked for, not a
description of what is running. `CONFIG GET` is what answers the second question.

The settings above are the exception, and they are the exception for one reason:
a fact the server needs *before* it exists cannot be a command it runs *after* it
does. A listen port is fixed the moment it is bound and the master is fixed the
moment the following service is built, so those lines are read first, by keyword,
and nothing else in the file is allowed to be: every other line is a command.
