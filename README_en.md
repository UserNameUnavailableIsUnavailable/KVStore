# KVStore

## Overview

KVStore is a lightweight RESP-compatible key/value server written in C++. It provides a simple command interface, multiple storage backends, optional persistence, and replication support for basic distributed usage.

## Key Features

- RESP-style command protocol compatible with Redis-style clients
- Multiple index implementations: array, hash, red-black-tree, and skip-list
- Two networking backends: epoll and io_uring
- Optional persistence with snapshots and append-only logs
- Basic replication via SLAVEOF and PSYNC

## Build

This project depends on vcpkg. Make sure VCPKG_ROOT is set, then build:

```bash
cmake -S . -B build
cmake --build build --target KVServer
```

## Run the Server

```bash
./build/Server/KVServer --port 8080 --networking-model epoll --cache-strategy hash
```

Useful options include:

- `--persistent-dir <dir>` for persistence storage
- `--allocator default|pool` to pick the allocation strategy
- `--memory-pooling` and `--memory-pool-size <bytes>`

Memory management is three orthogonal layers:

1. **Process allocator**: `malloc`/`free`, which is what `new`/`delete` route to.
   Swapping in jemalloc or tcmalloc is a link-time or deployment choice and needs
   no code: `LD_PRELOAD=libjemalloc.so.2 ./build/Server/KVServer`.
2. **PMR strategy**: take one large block from layer 1 and sub-allocate inside it
   with `std::pmr`, which is what `--allocator pool` selects.
3. **Object pool**: `Slab<T>` manages fixed-size objects of one type, handing out
   stable handles with O(1) acquire and release.

## Run the Client

```bash
./build/Client/KVClient 127.0.0.1 8080
```

You can then issue commands such as:

```text
SET foo bar
GET foo
DEL foo
EXPIRE foo 10
QUIT
```

## Basic Usage Notes

- `SAVE` writes a full dump to the persistence directory.
- `CONFIG SET appendonly yes` enables command logging for recovery, and `CONFIG GET appendonly` reports the current state.
- `SLAVEOF <host> <port>` starts replication from another server.

## Test

```bash
ctest --test-dir build/Common
```
