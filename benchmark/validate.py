#!/usr/bin/env python3
"""Read records back from a server and check them against what was written.

The pairs are not read from a file: they are generated again the way
`insert.py` generated them, so this only has to be told the same seed and count.
The first pair that does not match is the answer, so it stops there and exits
non-zero -- which is what makes it usable as the check after a restart.

    python benchmark/validate.py --host 127.0.0.1 --port 6666 --count 100000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark import DEFAULT_COUNT, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SEED, PROGRESS_EVERY, Meter, progress, records

import redis

# A value is worth showing whole in a one-line report up to this many
# characters. Its length is printed either way, which is what tells a cut-short
# value from a different one.
SHOWN = 60


def described(value: str | None) -> str:
    """A value the way a mismatch report says it: a key that is not there says so,
    and a long value is cut short rather than printed whole."""
    if value is None:
        return "nothing (the key is missing)"
    if len(value) <= SHOWN:
        return f"{len(value)} bytes {value!r}"
    return f"{len(value)} bytes {value[:SHOWN]!r}..."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate records held by a running server.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Server to read from")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port it serves clients on")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of records to check")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Seed the values were generated from")
    parser.add_argument("--server-pid", type=int, default=None,
                        help="Server process to report memory for (default: the process listening on the port)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = redis.Redis(host=args.host, port=args.port, decode_responses=True, protocol=2)
    meter = Meter(port=args.port, server_pid=args.server_pid)

    checked = 0
    for key, value in records(args.seed, args.count):
        actual = client.get(key)
        checked += 1
        if actual != value:
            # The first pair that does not match is the answer, so there is
            # nothing to learn from the rest of them and no reason to make the
            # caller wait while they are all read.
            print(flush=True)  # finish the line the progress was on
            print(f"mismatch at {key}: expected {described(value)}, found {described(actual)}", file=sys.stderr)
            print(f"the values come from --seed {args.seed}: both scripts have to be given the same one", file=sys.stderr)
            meter.report(checked)
            return 1
        if checked % PROGRESS_EVERY == 0 or checked == args.count:
            progress("validated", checked, args.count)
    print(flush=True)

    print(f"all {args.count} key-value pairs in {args.host}:{args.port} match", flush=True)
    meter.report(checked)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
