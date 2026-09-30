# Memory Results

The benchmark ran with `python3 benchmark/mem.py --count 1000000 --clients 1000 --sample-interval 0.01 --startup-timeout 10 --cycles 4`.

## No Memory Pooling

Initial RSS: 6.8 MiB

| Cycle | Peak RSS After Insert | RSS After Remove | Insert Time | Remove Time | Insert RPS | Remove RPS | Retained RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 385.2 MiB | 385.2 MiB | 44.00 s | 19.09 s | 22,728 | 52,376 | 378.5 MiB |
| 2 | 461.4 MiB | 385.3 MiB | 45.67 s | 20.15 s | 21,894 | 49,618 | 378.6 MiB |
| 3 | 470.0 MiB | 385.3 MiB | 45.73 s | 18.84 s | 21,867 | 53,090 | 378.6 MiB |
| 4 | 472.2 MiB | 385.3 MiB | 43.66 s | 18.88 s | 22,907 | 52,956 | 378.6 MiB |

## Custom Memory Pooling

Initial RSS: 6.8 MiB

| Cycle | Peak RSS After Insert | RSS After Remove | Insert Time | Remove Time | Insert RPS | Remove RPS | Retained RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 491.3 MiB | 491.3 MiB | 42.64 s | 20.28 s | 23,451 | 49,318 | 484.5 MiB |
| 2 | 563.9 MiB | 493.4 MiB | 45.61 s | 21.08 s | 21,924 | 47,436 | 486.6 MiB |
| 3 | 569.4 MiB | 493.4 MiB | 44.74 s | 19.59 s | 22,350 | 51,058 | 486.6 MiB |
| 4 | 566.3 MiB | 493.4 MiB | 43.73 s | 18.79 s | 22,870 | 53,219 | 486.6 MiB |

## jemalloc

Initial RSS: 7.4 MiB

| Cycle | Peak RSS After Insert | RSS After Remove | Insert Time | Remove Time | Insert RPS | Remove RPS | Retained RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 388.4 MiB | 373.4 MiB | 43.60 s | 17.31 s | 22,934 | 57,766 | 366.0 MiB |
| 2 | 442.1 MiB | 376.9 MiB | 43.45 s | 17.85 s | 23,017 | 56,013 | 369.5 MiB |
| 3 | 436.5 MiB | 377.2 MiB | 43.42 s | 18.19 s | 23,029 | 54,964 | 369.8 MiB |
| 4 | 432.9 MiB | 377.2 MiB | 43.78 s | 17.56 s | 22,840 | 56,934 | 369.7 MiB |