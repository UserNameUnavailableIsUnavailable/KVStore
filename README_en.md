# KVStore

**English** | [中文](README.md)

A RESP-compatible key/value server written in C++20: clients speak TCP and RESP, nodes
speak RDMA to each other, data lives in a snapshot (RDB) and an append-only log (AOF), and
writes reach the replicas as they happen.

## Build requirements

### System and toolchain

- **Operating system**: Linux (kernel version >= 5.2. 6.0 and higher are recommended for full io_uring features). The server, replication and the io_uring backend
  all sit behind `#if defined(__linux__)`; on other platforms only part of `NBIO`
  compiles.
- **CMake ≥ 3.20** (declared at the top of `CMakeLists.txt`; 4.2.3 on the development
  machine).
- **A C++20 compiler**: the code uses coroutines, ranges, designated initializers and
  concepts (Clang 21.1.8 on the development machine; GCC 11+ / Clang 14+ have what it
  needs).
- **Ninja** (optional — any CMake generator will do; 1.13.2 here).

### Linux development packages (required before configure)

RDMA, io_uring and BPF libraries are resolved from the system via `pkg-config`
(not from vcpkg). Install the development packages first:

```bash
sudo apt-get update
sudo apt-get install -y rdma-core libibverbs-dev librdmacm-dev liburing-dev libbpf-dev pkg-config
```

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

Linux system libraries resolved via `pkg-config` (must be installed before build):

| System package family | Used for |
| --- | --- |
| `librdmacm` + `libibverbs` (`librdmacm-dev`, `libibverbs-dev`) | RDMA replication and the wire tests |
| `liburing` (`liburing-dev`) | The io_uring multiplexer (`URingMultiplexer`) |
| `libbpf` (`libbpf-dev`) | BPF experiments |

### RDMA hardware (only replication needs it)

Replication and the `RDMA*` tests need an RDMA device, real or in software (`siw`'s
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
| `NBIOTesting`, `ServerTesting` | The test executables |
| `Echo`, `Sleep`, `Grace`, `SystemSignalSvc`, `ScopedSystemSignalService`, `Condition` | NBIO examples |
| `RdmaEcho`, `RdmaAsyncEcho` | RDMA examples |
| `Tutorial_RdmaServer`, `Tutorial_RdmaClient` | The raw verbs example in `Tutorial/RDMA` |
| `RdmaFileBenchmark` | RDMA link throughput at the NBIO layer, needs `-DKVSTORE_BUILD_BENCHMARKS=ON` |

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
    --port 8080 --replication-port 8081 --replication-ip 192.168.0.201 --rdma-device siw0
./build/Application/Server/Server --port 8082 --rdma-device siw0

# and the replica is pointed at the master at runtime, by a client
./build/Application/Client/Client 127.0.0.1 8082 SLAVEOF 192.168.0.201 8081

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
config rdma_device siw0
config appendonly yes
```

`Application/replica.conf` (a replica serving clients, before it is told which master):

```
config port 8082
config rdma_device siw0
```

## Benchmarking

`benchmark/` contains benchmark scripts and results.

| Benchmark | Script | Results |
| --- | --- | --- |
| Throughput under `redis-benchmark`: multiplexer comparison, pipeline depth (against Redis), AOF, periodic backup | `benchmark/pipeline.sh` and friends | [`benchmark/RSP.md`](benchmark/RSP.md) |
| Memory: RSS and reuse across repeated insert/remove cycles, against no pooling, custom pooling and jemalloc | `benchmark/memory.py` | [`benchmark/memory.md`](benchmark/memory.md) |

## Documentation

- `Documentation/REPLICATION.md` — RDMA full and incremental synchronization
- `Documentation/CONFIG.md` — the startup command file and its startup settings
- `Documentation/RDMA.md`, `NBIO/Core/RDMA.md` — the RDMA backend and runtime
- `Documentation/PSYNC.md` — the server-to-server protocol; `Documentation/SLAVEOF.md` —
  the command a client sends to start it
- `DESIGN.md`, `PITFALLS.md` — design decisions and the traps hit on the way
