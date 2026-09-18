#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark import DEFAULT_BUILD_DIRECTORY, PROJECT_ROOT, find_target_file
from benchmark.save import DEFAULT_COUNT, DEFAULT_PORT, DEFAULT_RESTART_WAIT, run_roundtrip

DEFAULT_CONFIG = PROJECT_ROOT / "benchmark" / "aof.conf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write data to the AOF, restart KVStore, and verify every recovered value.")
    parser.add_argument("--target", default="Server", help="CMake target for the KVStore server executable")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIRECTORY, help="CMake build directory")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Startup configuration that enables append-only persistence")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to run the server on; 0 selects a free port")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of key-value pairs to recover")
    parser.add_argument("--restart-wait", type=float, default=DEFAULT_RESTART_WAIT, help="Seconds to wait for server startup")
    parser.add_argument("--keep-temp", action="store_true", help="Keep the temporary AOF and server logs under PROJECT_ROOT/temp")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count <= 0 or args.port < 0 or args.port > 65535 or args.restart_wait <= 0:
        raise RuntimeError("count and restart wait must be positive, and port must be between 0 and 65535")
    if not args.config.is_file():
        raise RuntimeError(f"AOF configuration was not found: {args.config}")

    server = find_target_file(args.target, args.build_dir)
    run_roundtrip(server, args.port, args.count, args.restart_wait, "aof", keep_temp=args.keep_temp,
                  config_path=args.config.resolve())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"AOF recovery test failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error