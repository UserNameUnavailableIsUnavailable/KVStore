# Redis Comparison Report

Generated: 2026-08-09T09:02:50.245128+00:00

Redis: Redis server v=7.0.15 sha=00000000:0 malloc=jemalloc-5.3.0 bits=64 build=e53ff17674aa6190

Clients: 16; warmup: 1.0 s; measured duration: 3.0 s; repeats: 3

Both servers use the same non-pipelined SET workload. Redis persistence is disabled. KVStore values are means across all eight backend/index combinations.

| Value | Server | QPS | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |
|---:|---|---:|---:|---:|---:|---:|---:|
| 64 B | KVStore mean | 12068 | 1.20 | 1437.2 | 4.94 | 5.09 | 0.15 |
| 64 B | Redis | 75034 +/- 302 | 7.44 | 735.1 | 13.62 | 14.06 | 0.44 |
| 64 B | Redis vs KVStore | +521.79% | | | | | |
| 4096 B | KVStore mean | 6422 | 25.34 | 2621.1 | 4.92 | 6.14 | 1.22 |
| 4096 B | Redis | 74950 +/- 546 | 295.77 | 727.1 | 13.64 | 14.13 | 0.49 |
| 4096 B | Redis vs KVStore | +1067.06% | | | | | |
| 65536 B | KVStore mean | 813 | 50.84 | 20406.0 | 4.93 | 22.91 | 17.94 |
| 65536 B | Redis | 64392 +/- 222 | 4027.13 | 678.8 | 13.58 | 15.86 | 2.28 |
| 65536 B | Redis vs KVStore | +7821.62% | | | | | |
