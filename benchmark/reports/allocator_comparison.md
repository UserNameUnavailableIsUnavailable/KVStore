# KVStore Server Benchmark Report

Generated: 2026-08-09T08:28:57.764789+00:00

Clients: 16; warmup: 1.0 s; measured duration: 3.0 s; repeats: 3

Peak RSS is total resident memory for the server process, including the index, values, networking state, allocator overhead, and executable pages.

| Value | Store | Backend | Allocator | QPS mean +/- sd | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|
| 64 B | array | epoll | default | 11342 +/- 740 | 1.12 | 3551.2 | 5.37 | 5.58 | 0.21 |
| 64 B | array | epoll | jemalloc | 12002 +/- 101 | 1.19 | 3113.3 | 6.25 | 6.46 | 0.22 |
| 64 B | array | io_uring | default | 12074 +/- 164 | 1.20 | 3080.7 | 5.75 | 5.95 | 0.20 |
| 64 B | array | io_uring | jemalloc | 12077 +/- 19 | 1.20 | 3053.4 | 6.75 | 6.94 | 0.19 |
| 64 B | hash | epoll | default | 11950 +/- 134 | 1.19 | 3046.9 | 5.38 | 5.61 | 0.24 |
| 64 B | hash | epoll | jemalloc | 12003 +/- 27 | 1.19 | 3055.1 | 6.24 | 6.46 | 0.22 |
| 64 B | hash | io_uring | default | 11960 +/- 136 | 1.19 | 3084.4 | 5.79 | 5.99 | 0.21 |
| 64 B | hash | io_uring | jemalloc | 11827 +/- 103 | 1.17 | 3172.6 | 6.75 | 6.96 | 0.21 |
| 64 B | red-black-tree | epoll | default | 11895 +/- 51 | 1.18 | 3083.4 | 5.37 | 5.58 | 0.21 |
| 64 B | red-black-tree | epoll | jemalloc | 11894 +/- 78 | 1.18 | 3101.4 | 6.20 | 6.42 | 0.22 |
| 64 B | red-black-tree | io_uring | default | 11587 +/- 5 | 1.15 | 3260.0 | 5.80 | 6.01 | 0.21 |
| 64 B | red-black-tree | io_uring | jemalloc | 11497 +/- 190 | 1.14 | 3286.6 | 6.74 | 6.95 | 0.21 |
| 64 B | skip-list | epoll | default | 11882 +/- 23 | 1.18 | 3120.6 | 5.39 | 5.60 | 0.21 |
| 64 B | skip-list | epoll | jemalloc | 11684 +/- 102 | 1.16 | 3196.0 | 6.23 | 6.48 | 0.25 |
| 64 B | skip-list | io_uring | default | 10993 +/- 642 | 1.09 | 3653.5 | 5.76 | 5.98 | 0.21 |
| 64 B | skip-list | io_uring | jemalloc | 11473 +/- 139 | 1.14 | 3272.0 | 6.78 | 6.97 | 0.19 |
| 4096 B | array | epoll | default | 11092 +/- 61 | 43.77 | 3326.3 | 5.37 | 6.68 | 1.31 |
| 4096 B | array | epoll | jemalloc | 11057 +/- 143 | 43.64 | 3335.7 | 6.27 | 7.81 | 1.54 |
| 4096 B | array | io_uring | default | 11108 +/- 149 | 43.83 | 3353.8 | 5.83 | 7.09 | 1.26 |
| 4096 B | array | io_uring | jemalloc | 11055 +/- 47 | 43.63 | 3421.6 | 6.76 | 8.30 | 1.54 |
| 4096 B | hash | epoll | default | 11184 +/- 27 | 44.14 | 3334.4 | 5.35 | 6.63 | 1.28 |
| 4096 B | hash | epoll | jemalloc | 11060 +/- 57 | 43.65 | 3351.5 | 6.25 | 7.80 | 1.55 |
| 4096 B | hash | io_uring | default | 10997 +/- 81 | 43.40 | 3392.2 | 5.85 | 7.11 | 1.26 |
| 4096 B | hash | io_uring | jemalloc | 10873 +/- 115 | 42.91 | 3459.0 | 6.77 | 8.30 | 1.53 |
| 4096 B | red-black-tree | epoll | default | 10937 +/- 83 | 43.16 | 3375.9 | 5.36 | 6.65 | 1.29 |
| 4096 B | red-black-tree | epoll | jemalloc | 10987 +/- 85 | 43.36 | 3353.0 | 6.27 | 7.81 | 1.54 |
| 4096 B | red-black-tree | io_uring | default | 10969 +/- 94 | 43.29 | 3368.4 | 5.79 | 7.06 | 1.28 |
| 4096 B | red-black-tree | io_uring | jemalloc | 11064 +/- 49 | 43.66 | 3313.2 | 6.75 | 8.30 | 1.55 |
| 4096 B | skip-list | epoll | default | 11073 +/- 64 | 43.70 | 3310.5 | 5.42 | 6.70 | 1.28 |
| 4096 B | skip-list | epoll | jemalloc | 10803 +/- 258 | 42.63 | 3565.7 | 6.29 | 7.85 | 1.57 |
| 4096 B | skip-list | io_uring | default | 10890 +/- 76 | 42.98 | 3423.9 | 5.80 | 7.08 | 1.28 |
| 4096 B | skip-list | io_uring | jemalloc | 11028 +/- 123 | 43.52 | 3363.3 | 6.79 | 8.33 | 1.55 |
| 65536 B | array | epoll | default | 1882 +/- 16 | 117.70 | 10056.9 | 5.36 | 23.38 | 17.97 |
| 65536 B | array | epoll | jemalloc | 1887 +/- 15 | 118.02 | 9842.6 | 6.25 | 27.50 | 21.22 |
| 65536 B | array | io_uring | default | 1991 +/- 13 | 124.53 | 9042.8 | 5.78 | 23.80 | 18.02 |
| 65536 B | array | io_uring | jemalloc | 2012 +/- 17 | 125.83 | 8878.1 | 6.75 | 28.00 | 21.11 |
| 65536 B | hash | epoll | default | 1981 +/- 52 | 123.89 | 9217.1 | 5.40 | 23.48 | 18.06 |
| 65536 B | hash | epoll | jemalloc | 1931 +/- 23 | 120.77 | 9723.7 | 6.26 | 27.55 | 21.23 |
| 65536 B | hash | io_uring | default | 1923 +/- 17 | 120.27 | 9522.7 | 5.80 | 23.83 | 18.03 |
| 65536 B | hash | io_uring | jemalloc | 1924 +/- 38 | 120.32 | 9664.9 | 6.75 | 28.07 | 21.22 |
| 65536 B | red-black-tree | epoll | default | 1958 +/- 20 | 122.48 | 9564.9 | 5.35 | 23.38 | 17.94 |
| 65536 B | red-black-tree | epoll | jemalloc | 1923 +/- 25 | 120.29 | 9686.1 | 6.29 | 27.55 | 21.23 |
| 65536 B | red-black-tree | io_uring | default | 1966 +/- 25 | 122.96 | 9270.2 | 5.84 | 23.89 | 18.04 |
| 65536 B | red-black-tree | io_uring | jemalloc | 1946 +/- 45 | 121.69 | 9572.4 | 6.76 | 28.00 | 21.14 |
| 65536 B | skip-list | epoll | default | 1932 +/- 34 | 120.84 | 9726.8 | 5.37 | 23.46 | 18.02 |
| 65536 B | skip-list | epoll | jemalloc | 1965 +/- 24 | 122.88 | 9454.3 | 6.25 | 27.55 | 21.28 |
| 65536 B | skip-list | io_uring | default | 1984 +/- 5 | 124.08 | 9868.6 | 5.81 | 23.88 | 18.07 |
| 65536 B | skip-list | io_uring | jemalloc | 1978 +/- 17 | 123.71 | 9495.2 | 6.74 | 27.98 | 21.13 |

## Backend Difference

Positive values mean io_uring achieved higher QPS than epoll.

| Value | Store | io_uring QPS delta |
|---:|---|---:|
| 64 B / default | array | +6.46% |
| 64 B / default | hash | +0.08% |
| 64 B / default | red-black-tree | -2.58% |
| 64 B / default | skip-list | -7.48% |
| **64 B / default** | **mean across stores** | **-0.88%** |
| 4096 B / default | array | +0.14% |
| 4096 B / default | hash | -1.67% |
| 4096 B / default | red-black-tree | +0.29% |
| 4096 B / default | skip-list | -1.65% |
| **4096 B / default** | **mean across stores** | **-0.72%** |
| 65536 B / default | array | +5.81% |
| 65536 B / default | hash | -2.93% |
| 65536 B / default | red-black-tree | +0.39% |
| 65536 B / default | skip-list | +2.68% |
| **65536 B / default** | **mean across stores** | **+1.49%** |
| 64 B / jemalloc | array | +0.63% |
| 64 B / jemalloc | hash | -1.46% |
| 64 B / jemalloc | red-black-tree | -3.33% |
| 64 B / jemalloc | skip-list | -1.81% |
| **64 B / jemalloc** | **mean across stores** | **-1.49%** |
| 4096 B / jemalloc | array | -0.02% |
| 4096 B / jemalloc | hash | -1.69% |
| 4096 B / jemalloc | red-black-tree | +0.71% |
| 4096 B / jemalloc | skip-list | +2.08% |
| **4096 B / jemalloc** | **mean across stores** | **+0.27%** |
| 65536 B / jemalloc | array | +6.62% |
| 65536 B / jemalloc | hash | -0.37% |
| 65536 B / jemalloc | red-black-tree | +1.17% |
| 65536 B / jemalloc | skip-list | +0.67% |
| **65536 B / jemalloc** | **mean across stores** | **+2.02%** |

## Allocator Difference

Positive values mean jemalloc achieved higher QPS than the default allocator.

| Value | Store | Backend | jemalloc QPS delta |
|---:|---|---|---:|
| 64 B | array | epoll | +5.82% |
| 64 B | array | io_uring | +0.03% |
| 64 B | hash | epoll | +0.44% |
| 64 B | hash | io_uring | -1.11% |
| 64 B | red-black-tree | epoll | -0.01% |
| 64 B | red-black-tree | io_uring | -0.78% |
| 64 B | skip-list | epoll | -1.67% |
| 64 B | skip-list | io_uring | +4.37% |
| **64 B** | **mean across stores/backends** | **all** | **+0.89%** |
| 4096 B | array | epoll | -0.31% |
| 4096 B | array | io_uring | -0.47% |
| 4096 B | hash | epoll | -1.11% |
| 4096 B | hash | io_uring | -1.13% |
| 4096 B | red-black-tree | epoll | +0.45% |
| 4096 B | red-black-tree | io_uring | +0.87% |
| 4096 B | skip-list | epoll | -2.44% |
| 4096 B | skip-list | io_uring | +1.26% |
| **4096 B** | **mean across stores/backends** | **all** | **-0.36%** |
| 65536 B | array | epoll | +0.28% |
| 65536 B | array | io_uring | +1.04% |
| 65536 B | hash | epoll | -2.52% |
| 65536 B | hash | io_uring | +0.05% |
| 65536 B | red-black-tree | epoll | -1.79% |
| 65536 B | red-black-tree | io_uring | -1.03% |
| 65536 B | skip-list | epoll | +1.68% |
| 65536 B | skip-list | io_uring | -0.30% |
| **65536 B** | **mean across stores/backends** | **all** | **-0.32%** |
