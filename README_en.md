# KVStore

**English** | [中文](README.md)

A RESP-compatible key/value server written in C++20: clients speak TCP and RESP, nodes
speak RDMA to each other, data lives in a snapshot (RDB) and an append-only log (AOF), and
writes reach the replicas as they happen.

## Build requirements

### System and toolchain

- **Operating system**: Linux (x86-64). The server, replication and the io_uring backend
  all sit behind `#if defined(__linux__)`; on other platforms only part of `Foundation`
  compiles.
- **CMake ≥ 3.20** (declared at the top of `CMakeLists.txt`; 4.2.3 on the development
  machine).
- **A C++20 compiler**: the code uses coroutines, ranges, designated initializers and
  concepts (Clang 21.1.8 on the development machine; GCC 11+ / Clang 14+ have what it
  needs).
- **Ninja** (optional — any CMake generator will do; 1.13.2 here).

### vcpkg and third-party libraries

The project uses vcpkg in **manifest mode**: the dependencies are in `vcpkg.json`, the
installed tree is pinned to `vcpkg_installed/` inside the repository, and the triplet is
`x64-linux`. All a build needs is `VCPKG_ROOT` pointing at a local vcpkg — the top-level
`CMakeLists.txt` sets `CMAKE_TOOLCHAIN_FILE` from it, and CMake installs and builds
everything below by itself.

| Dependency | Used for |
| --- | --- |
| `cli11` | Command line parsing (`Server`, `Client`) |
| `spdlog` | Logging |
| `gtest` | Unit tests (`BUILD_TESTING`) |
| `benchmark` | Benchmarks (Google Benchmark) |
| `crcpp` | The CRC-64 that a snapshot file is validated against |
| `nlohmann-json` | JSON |
| `jemalloc` | An optional process allocator (`LD_PRELOAD`) |
| `rdma-core` (Linux) | `libibverbs` / `librdmacm`: RDMA replication and the wire tests |
| `liburing` (Linux) | The io_uring multiplexer (`URingMultiplexer`) |
| `libbpf` (Linux) | BPF experiments |

### RDMA hardware (only replication needs it)

Replication and the `RDMA_*` tests need an RDMA device, real or in software (`siw`'s
`siw0`, address `192.168.0.201`, on the development machine). Without one the server still
runs stand-alone, and the **four RDMA wire tests skip by design**: they take the device's
address from `KVSTORE_RDMA_ADDRESS`, because that address cannot be derived portably.

## Building

```bash
export VCPKG_ROOT=/path/to/vcpkg     # required: the top-level CMakeLists reads it
cmake -S . -B build -G Ninja         # the first configure installs and builds the deps
cmake --build build                  # or one target: --target Server
```

The `build/` in the repository is a Debug tree; add `-DCMAKE_BUILD_TYPE=Release` before
measuring anything.

Targets worth knowing:

| Target | What it is |
| --- | --- |
| `Server` | The server: `build/Application/Server/Server` |
| `Client` | The command line client: `build/Application/Client/Client` |
| `FoundationTesting`, `ServerTesting` | The test executables |
| `Echo`, `Sleep`, `Grace`, `SignalSvc`, `ScopedSignalService`, `Condition` | NBIO examples |
| `RDMA_Echo`, `RDMA_AsyncEcho` | RDMA examples |
| `Tutorial_RDMA_Server`, `Tutorial_RDMA_Client` | The raw verbs example in `Tutorial/RDMA` |

## Testing

```bash
ctest --test-dir build --output-on-failure     # everything; the four RDMA tests skip

export KVSTORE_RDMA_ADDRESS=192.168.0.201      # with a device, so they really run
ctest --test-dir build --output-on-failure
```

`KVSTORE_LOG_LEVEL=debug|info|warn` sets the log level; at `debug` the transfer logs every
packet and every credit.

## Running

```bash
# stand-alone
./build/Application/Server/Server --port 8080

# a master and a replica on one machine
./build/Application/Server/Server \
    --port 8080 --replication-port 8081 --replication-address 192.168.0.201
./build/Application/Server/Server \
    --port 8082 --replicaof 192.168.0.201:8081

# the client: Client [host] [port], 127.0.0.1:6379 by default
./build/Application/Client/Client 127.0.0.1 8080
```

A server can also be driven entirely by a startup command file, in which every line is a
command — anything a connection may send, a file may say:

```bash
./build/Application/Server/Server -c Application/master.conf
```

`Application/master.conf` (serves clients, and replicas on the RDMA device):

```
config port 8080
config replication_address 192.168.0.201 8081
config appendonly yes
```

`Application/replica.conf` (a read-only replica following that master):

```
config port 8082
replicaof 192.168.0.201 8081
```

Things that catch people out:

- The address in `--replication-address` / `config replication_address` has to be the **RDMA
  device's own address**. A wildcard such as `0.0.0.0` binds no device, and the server
  refuses to start rather than come up unable to serve replicas.
- Two instances on one machine cannot share a client port (both default to 8080).
- A replica is read-only to its clients — writes get `-READONLY` — while the writes its
  master sends are applied as they arrive.
- Snapshots and the AOF are written to the process's working directory: `dump.rdb`,
  `appendonly.aof`.

## Documentation

- `Documentation/REPLICATION.md` — RDMA full and incremental synchronization
- `Documentation/CONFIG.md` — the startup command file and its two startup settings
- `Documentation/RDMA.md`, `Foundation/Core/RDMA.md` — the RDMA backend and runtime
- `Documentation/PSYNC.md`, `Documentation/SLAVEOF.md` — an earlier TCP replication design
  (not implemented)
- `DESIGN.md`, `PITFALLS.md` — design decisions and the traps hit on the way
