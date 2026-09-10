# Redis Comparison Report

Generated: 2026-08-09T08:09:15.311938+00:00

Redis: Redis server v=8.0.5 sha=00000000:0 malloc=jemalloc-5.3.0 bits=64 build=9729964261b8fc0f

Clients: 16; warmup: 1.0 s; measured duration: 3.0 s; repeats: 3

Both servers use the same non-pipelined SET workload. Redis persistence is disabled. KVStore values are means across all eight backend/index combinations.

| Value | Server | QPS | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |
|---:|---|---:|---:|---:|---:|---:|---:|
| 64 B | KVStore mean | 11923 | 1.18 | 3108.3 | 5.56 | 5.77 | 0.21 |
| 64 B | Redis | 12225 +/- 144 | 1.21 | 2994.3 | 14.60 | 14.96 | 0.36 |
| 64 B | Redis vs KVStore | +2.53% | | | | | |
| 4096 B | KVStore mean | 11173 | 44.09 | 3320.1 | 5.57 | 6.85 | 1.27 |
| 4096 B | Redis | 12181 +/- 138 | 48.07 | 3010.7 | 14.76 | 15.11 | 0.36 |
| 4096 B | Redis vs KVStore | +9.02% | | | | | |
| 65536 B | KVStore mean | 1988 | 124.32 | 9483.1 | 5.57 | 23.62 | 18.00 |
| 65536 B | Redis | 10798 +/- 120 | 675.30 | 3537.8 | 14.74 | 16.80 | 2.06 |
| 65536 B | Redis vs KVStore | +443.20% | | | | | |
