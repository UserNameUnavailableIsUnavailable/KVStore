# KVStore Server Benchmark Report

Generated: 2026-08-09T09:01:15.957013+00:00

Clients: 16; warmup: 1.0 s; measured duration: 3.0 s; repeats: 3

Peak RSS is total resident memory for the server process, including the index, values, networking state, allocator overhead, and executable pages.

| Value | Store | Backend | Allocator | QPS mean +/- sd | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|
| 64 B | array | epoll | default | 11928 +/- 28 | 1.18 | 1499.5 | 4.71 | 4.85 | 0.14 |
| 64 B | array | io_uring | default | 12160 +/- 51 | 1.21 | 1389.6 | 5.14 | 5.30 | 0.17 |
| 64 B | hash | epoll | default | 11963 +/- 90 | 1.19 | 1519.5 | 4.72 | 4.89 | 0.16 |
| 64 B | hash | io_uring | default | 12202 +/- 22 | 1.21 | 1381.3 | 5.17 | 5.31 | 0.15 |
| 64 B | red-black-tree | epoll | default | 11990 +/- 34 | 1.19 | 1460.7 | 4.70 | 4.85 | 0.15 |
| 64 B | red-black-tree | io_uring | default | 12128 +/- 6 | 1.20 | 1388.4 | 5.16 | 5.30 | 0.14 |
| 64 B | skip-list | epoll | default | 11992 +/- 61 | 1.19 | 1473.8 | 4.73 | 4.87 | 0.14 |
| 64 B | skip-list | io_uring | default | 12178 +/- 36 | 1.21 | 1384.6 | 5.18 | 5.33 | 0.14 |
| 4096 B | array | epoll | default | 6373 +/- 22 | 25.15 | 2649.2 | 4.68 | 5.91 | 1.23 |
| 4096 B | array | io_uring | default | 6478 +/- 45 | 25.56 | 2561.1 | 5.15 | 6.37 | 1.21 |
| 4096 B | hash | epoll | default | 6346 +/- 13 | 25.04 | 2696.4 | 4.71 | 5.94 | 1.23 |
| 4096 B | hash | io_uring | default | 6482 +/- 20 | 25.58 | 2554.6 | 5.16 | 6.38 | 1.21 |
| 4096 B | red-black-tree | epoll | default | 6361 +/- 21 | 25.10 | 2685.1 | 4.67 | 5.89 | 1.22 |
| 4096 B | red-black-tree | io_uring | default | 6479 +/- 10 | 25.57 | 2597.3 | 5.17 | 6.38 | 1.21 |
| 4096 B | skip-list | epoll | default | 6381 +/- 6 | 25.18 | 2663.3 | 4.66 | 5.90 | 1.24 |
| 4096 B | skip-list | io_uring | default | 6478 +/- 3 | 25.56 | 2562.2 | 5.15 | 6.37 | 1.22 |
| 65536 B | array | epoll | default | 812 +/- 6 | 50.77 | 20622.0 | 4.70 | 22.74 | 17.86 |
| 65536 B | array | io_uring | default | 814 +/- 6 | 50.92 | 20183.6 | 5.16 | 23.12 | 17.95 |
| 65536 B | hash | epoll | default | 815 +/- 3 | 50.99 | 20269.1 | 4.69 | 22.65 | 17.87 |
| 65536 B | hash | io_uring | default | 808 +/- 2 | 50.52 | 20590.3 | 5.15 | 23.11 | 17.96 |
| 65536 B | red-black-tree | epoll | default | 819 +/- 2 | 51.25 | 20285.5 | 4.67 | 22.76 | 18.06 |
| 65536 B | red-black-tree | io_uring | default | 816 +/- 0 | 51.02 | 20199.8 | 5.17 | 23.12 | 17.94 |
| 65536 B | skip-list | epoll | default | 807 +/- 2 | 50.47 | 20872.7 | 4.69 | 22.64 | 17.90 |
| 65536 B | skip-list | io_uring | default | 812 +/- 3 | 50.76 | 20224.8 | 5.19 | 23.14 | 17.96 |

## Backend Difference

Positive values mean io_uring achieved higher QPS than epoll.

| Value | Store | io_uring QPS delta |
|---:|---|---:|
| 64 B / default | array | +1.95% |
| 64 B / default | hash | +2.00% |
| 64 B / default | red-black-tree | +1.15% |
| 64 B / default | skip-list | +1.54% |
| **64 B / default** | **mean across stores** | **+1.66%** |
| 4096 B / default | array | +1.64% |
| 4096 B / default | hash | +2.15% |
| 4096 B / default | red-black-tree | +1.85% |
| 4096 B / default | skip-list | +1.52% |
| **4096 B / default** | **mean across stores** | **+1.79%** |
| 65536 B / default | array | +0.30% |
| 65536 B / default | hash | -0.93% |
| 65536 B / default | red-black-tree | -0.45% |
| 65536 B / default | skip-list | +0.56% |
| **65536 B / default** | **mean across stores** | **-0.13%** |
