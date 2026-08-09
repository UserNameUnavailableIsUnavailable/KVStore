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
- `--allocator default|pool|jemalloc`
- `--memory-pooling` and `--memory-pool-size <bytes>`

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
- `APPENDONLY YES` enables command logging for recovery.
- `SLAVEOF <host> <port>` starts replication from another server.

## Test

```bash
ctest --test-dir build/Common
```