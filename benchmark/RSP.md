# RPS

Environment: 13900HX + 32GiB RAM

## Different Pipeline Depths

Measured on 2026-10-18, approximately 02:52-02:59 UTC+08:00.
KVStore uses the optimized Release binary with **io_uring**, response batching,
and immediate nonblocking batch sends with asynchronous fallback on backpressure.
Redis server and redis-benchmark are version 8.0.5.

| Setting | Value |
|---|---|
| Host | Intel Core i9-13900HX, 31 GiB RAM reported by Linux |
| Kernel | Linux 7.0.0-38-generic, x86_64 |
| Connections | 50 persistent clients, single-threaded benchmark client |
| Transport | IPv4 TCP loopback, RESP2 |
| Dataset | 100,000 preloaded keys, 128-byte values, uniform random access |
| Requests | 5,000,000 per command, server, depth, and repetition |
| Repetitions | 3, seeds 43/44/45; server order alternates |
| Warm-up | 200,000 SETs and GETs per server, P=40, seed 42 |
| Persistence / replication | Disabled on both isolated instances |
| Summary | Median RPS and separately median reported p50 over three repetitions |

Dedicated servers used ports 16666 (KVStore) and 16379 (Redis), with initially
empty working directories. All 100,000 keys were explicitly preloaded before
warm-up; GETs therefore hit existing 128-byte values. Both final DBSIZE values
were 100,000. Redis used `io-threads 1` and its normal reply handling.

Measured commands (`PORT=16666` or `16379`, `P=1,10,20,40,80,160`,
`SEED=43,44,45`; `VALUE` is 128 ASCII `x` characters):

```bash
redis-benchmark -h 127.0.0.1 -p "$PORT" -c 50 -n 5000000 \
  -P "$P" -r 100000 --seed "$SEED" --csv \
  SET 'nbio:pipeline:__rand_int__' "$VALUE"
redis-benchmark -h 127.0.0.1 -p "$PORT" -c 50 -n 5000000 \
  -P "$P" -r 100000 --seed "$SEED" --csv \
  GET 'nbio:pipeline:__rand_int__'
```

### SET

| P | KVStore io_uring (req/s) | KVStore p50 (msec) | Redis (req/s) | Redis p50 (msec) | KVStore / Redis |
|---:|---:|---:|---:|---:|---:|
| 1 | 219,375 | 0.127 | 230,478 | 0.111 | 0.952x |
| 10 | 1,405,679 | 0.351 | 1,252,819 | 0.359 | 1.122x |
| 20 | 1,823,487 | 0.543 | 1,620,220 | 0.599 | 1.125x |
| 40 | 2,136,752 | 0.935 | 1,838,911 | 1.031 | 1.162x |
| 80 | 2,380,953 | 1.679 | 2,039,152 | 1.871 | 1.168x |
| 160 | 2,561,476 | 3.127 | 2,052,545 | 2.399 | 1.248x |

### GET

| P | KVStore io_uring (req/s) | KVStore p50 (msec) | Redis (req/s) | Redis p50 (msec) | KVStore / Redis |
|---:|---:|---:|---:|---:|---:|
| 1 | 227,015 | 0.119 | 230,023 | 0.111 | 0.987x |
| 10 | 1,421,666 | 0.351 | 1,378,930 | 0.319 | 1.031x |
| 20 | 1,881,822 | 0.527 | 1,795,977 | 0.527 | 1.048x |
| 40 | 2,267,574 | 0.879 | 2,087,683 | 0.911 | 1.086x |
| 80 | 2,566,735 | 1.559 | 2,283,105 | 1.663 | 1.124x |
| 160 | 2,735,230 | 2.927 | 2,391,200 | 3.135 | 1.144x |

All 72 measured runs and full reported latency statistics are in
[redis-kvstore-io-uring.csv](redis-kvstore-io-uring.csv). RPS is rounded to whole
requests/second; ratios use unrounded medians.

KVStore led both commands at all tested pipelined depths (P>=10). At P=160,
median throughput was 24.8% higher for SET and 14.4% higher for GET. Redis
remained ahead at P=1. This longer rerun supersedes any expectation from the
earlier 1,000,000-request experiment that io_uring wins every comparison.

Binary: `build/source/server/Release/server`, SHA-256
`edb9a89322bdb22369e4b8118e8a7d3f8c60b74185c0e919e0f95d924ec0d527`.
The checkout contains uncommitted changes and uses its installed NBIO dependency.
This is a shared-host, unpinned, warm in-memory benchmark, not a durability,
replication, eviction, insertion-growth, or remote-network test. Small differences
may reflect run-to-run noise. The reported pipeline p50 is redis-benchmark's
metric, not isolated server execution time. Dedicated instances were stopped
after the run.

## AOF

The following AOF and backup measurements are historical; they were not rerun
as part of the io_uring pipeline comparison above.

| AOF mode | SET (req/s) | SET p50 (msec) | GET (req/s) | GET p50 (msec) |
|---|---:|---:|---:|---:|
| no checksum | 180,453.30 | 0.255 | 227,308.89 | 0.207 |
| per command checksum | 168,958.88 | 0.271 | 219,038.86 | 0.215 |

## Periodic backup

`SAVE 0 <N>`: backup immediately once `N` records are updated. Backup runs in background.

| N | RPS (req/s) |
|---:|---:|
| 1,000 | 105,000.11 |
| 10,000 | 103,750.58 |
| 100,000 | 112,132.77 |
| 1,000,000 | 175,577.20 |