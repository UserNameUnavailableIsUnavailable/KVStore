#!/usr/bin/env python3
"""Write records into a server that is already running.

Nothing is kept on this side: the pairs come from a seed and a count, so
`validate.py` can generate the same ones and the data never has to be stored
anywhere but in the server.

    python benchmark/insert.py --host 127.0.0.1 --port 6666 --count 100000

A range can be written on its own: `--start` is the number the keys begin at, so
a second run adds `item:100000` upwards without rewriting what is already there.

    python benchmark/insert.py --host 127.0.0.1 --port 6666 --count 100000 --start 100000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark import DEFAULT_COUNT, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SEED, PROGRESS_EVERY, Meter, progress, records

import redis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Insert records into a running server.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Server to write to")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port it serves clients on")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of records to write")
    parser.add_argument("--start", type=int, default=0, help="Number the keys begin at (item:0 by default)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Seed the values are generated from")
    parser.add_argument("--server-pid", type=int, default=None,
                        help="Server process to report memory for (default: the process listening on the port)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = redis.Redis(host=args.host, port=args.port, decode_responses=True, protocol=2)
    meter = Meter(port=args.port, server_pid=args.server_pid)

    written = 0
    for key, value in records(args.seed, args.count, args.start):
        if not client.set(key, value):
            print(flush=True)
            print(f"SET {key} failed", file=sys.stderr)
            return 1
        written += 1
        if written % PROGRESS_EVERY == 0 or written == args.count:
            progress("inserted", written, args.count)
    print(flush=True)

    written_span = f"item:{args.start}..item:{args.start + written - 1}" if written else "nothing"
    print(f"inserted {written} key-value pairs ({written_span}) into {args.host}:{args.port}", flush=True)
    meter.report(written)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
