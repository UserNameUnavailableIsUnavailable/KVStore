# KVStore Server Benchmark Report

Generated: 2026-08-09T08:06:49.657421+00:00

Clients: 16; warmup: 1.0 s; measured duration: 3.0 s; repeats: 3

Peak RSS is total resident memory for the server process, including the index, values, networking state, allocator overhead, and executable pages.

| Value | Store | Backend | QPS mean +/- sd | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 64 B | array | epoll | 12013 +/- 124 | 1.19 | 3078.2 | 5.36 | 5.57 | 0.21 |
| 64 B | array | io_uring | 12001 +/- 62 | 1.19 | 3088.3 | 5.73 | 5.94 | 0.21 |
| 64 B | hash | epoll | 11837 +/- 185 | 1.17 | 3193.6 | 5.32 | 5.55 | 0.23 |
| 64 B | hash | io_uring | 11765 +/- 86 | 1.17 | 3160.3 | 5.76 | 5.97 | 0.21 |
| 64 B | red-black-tree | epoll | 11906 +/- 60 | 1.18 | 3110.6 | 5.35 | 5.55 | 0.21 |
| 64 B | red-black-tree | io_uring | 12002 +/- 53 | 1.19 | 3088.7 | 5.79 | 5.99 | 0.20 |
| 64 B | skip-list | epoll | 11918 +/- 58 | 1.18 | 3082.8 | 5.39 | 5.60 | 0.21 |
| 64 B | skip-list | io_uring | 11942 +/- 59 | 1.18 | 3063.6 | 5.79 | 5.99 | 0.21 |
| 4096 B | array | epoll | 11316 +/- 71 | 44.66 | 3213.7 | 5.34 | 6.61 | 1.28 |
| 4096 B | array | io_uring | 11087 +/- 243 | 43.75 | 3465.9 | 5.80 | 7.06 | 1.26 |
| 4096 B | hash | epoll | 11131 +/- 143 | 43.92 | 3382.2 | 5.33 | 6.61 | 1.29 |
| 4096 B | hash | io_uring | 11163 +/- 59 | 44.05 | 3315.3 | 5.81 | 7.08 | 1.26 |
| 4096 B | red-black-tree | epoll | 11199 +/- 4 | 44.19 | 3251.8 | 5.34 | 6.64 | 1.30 |
| 4096 B | red-black-tree | io_uring | 11249 +/- 29 | 44.39 | 3265.8 | 5.79 | 7.04 | 1.25 |
| 4096 B | skip-list | epoll | 11115 +/- 46 | 43.86 | 3341.4 | 5.37 | 6.64 | 1.27 |
| 4096 B | skip-list | io_uring | 11126 +/- 32 | 43.91 | 3324.8 | 5.82 | 7.10 | 1.28 |
| 65536 B | array | epoll | 1902 +/- 26 | 118.95 | 10226.0 | 5.36 | 23.35 | 17.85 |
| 65536 B | array | io_uring | 1961 +/- 20 | 122.65 | 9770.5 | 5.78 | 23.83 | 18.05 |
| 65536 B | hash | epoll | 1992 +/- 12 | 124.57 | 9336.3 | 5.38 | 23.46 | 18.04 |
| 65536 B | hash | io_uring | 1989 +/- 16 | 124.37 | 9477.5 | 5.82 | 23.72 | 17.90 |
| 65536 B | red-black-tree | epoll | 1995 +/- 21 | 124.76 | 9463.7 | 5.36 | 23.46 | 18.00 |
| 65536 B | red-black-tree | io_uring | 2034 +/- 9 | 127.19 | 8954.0 | 5.78 | 23.92 | 18.14 |
| 65536 B | skip-list | epoll | 2009 +/- 16 | 125.64 | 9486.2 | 5.31 | 23.30 | 17.91 |
| 65536 B | skip-list | io_uring | 2022 +/- 25 | 126.43 | 9150.4 | 5.81 | 23.91 | 18.11 |

## Backend Difference

Positive values mean io_uring achieved higher QPS than epoll.

| Value | Store | io_uring QPS delta |
|---:|---|---:|
| 64 B | array | -0.10% |
| 64 B | hash | -0.61% |
| 64 B | red-black-tree | +0.80% |
| 64 B | skip-list | +0.20% |
| **64 B** | **mean across stores** | **+0.07%** |
| 4096 B | array | -2.02% |
| 4096 B | hash | +0.29% |
| 4096 B | red-black-tree | +0.45% |
| 4096 B | skip-list | +0.10% |
| **4096 B** | **mean across stores** | **-0.30%** |
| 65536 B | array | +3.11% |
| 65536 B | hash | -0.16% |
| 65536 B | red-black-tree | +1.94% |
| 65536 B | skip-list | +0.63% |
| **65536 B** | **mean across stores** | **+1.38%** |
