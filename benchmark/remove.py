#!/usr/bin/env python3
"""Remove the records `insert.py` wrote.

The keys are generated the way `insert.py` generated them, so this deletes
exactly what that script inserted and nothing else: the server has no `KEYS` or
`SCAN`, and it does not need one for a set of keys that can be counted.

    python benchmark/remove.py --host 127.0.0.1 --port 6666 --count 100000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark import DEFAULT_COUNT, DEFAULT_HOST, DEFAULT_PORT, PROGRESS_EVERY, Meter, keys, progress

import redis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove the records a run inserted.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Server to delete from")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port it serves clients on")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of records to remove")
    parser.add_argument("--start", type=int, default=0, help="Number the keys begin at (item:0 by default)")
    parser.add_argument("--server-pid", type=int, default=None,
                        help="Server process to report memory for (default: the process listening on the port)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = redis.Redis(host=args.host, port=args.port, decode_responses=True, protocol=2)
    meter = Meter(port=args.port, server_pid=args.server_pid)

    removed = 0
    for index, key in enumerate(keys(args.count, args.start), start=1):
        # DEL answers with how many keys it removed, which is 0 for one that was
        # never there -- so the count says what was actually in the server.
        removed += int(client.delete(key))
        if index % PROGRESS_EVERY == 0 or index == args.count:
            progress("removed", index, args.count)
    print(flush=True)

    print(f"removed {removed} of {args.count} key-value pairs from {args.host}:{args.port}", flush=True)
    meter.report(args.count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
