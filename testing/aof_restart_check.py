#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
import signal
import string
import subprocess
import sys
import tempfile
import time
import socket
from pathlib import Path

try:
    import redis
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit("This script requires the 'redis' Python package. Install it with 'pip install redis'.") from exc

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_SERVER = Path(f"{PROJECT_ROOT}/build/Application/Server/Server")
DEFAULT_PORT = 0
DEFAULT_COUNT = 50000
DEFAULT_RESTART_WAIT = 30.0
DEFAULT_SEED = 0x6B7673746F7265
BATCH_SIZE = 1000

def random_value(rng: random.Random, index: int) -> str:
    alphabet = string.ascii_letters + string.digits
    size = 1 + (index % 7) * 11 + rng.randrange(0, 64)
    return "".join(rng.choice(alphabet) for _ in range(size))


def wait_for_server(client: redis.Redis, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if client.ping():
                return
        except Exception as exc:  # pragma: no cover - runtime polling
            last_error = exc
            time.sleep(0.1)
    raise RuntimeError(f"server did not become ready within {timeout:.1f}s") from last_error


def start_server(server_path: Path, port: int, cwd: Path, stdout_path: Path, stderr_path: Path) -> subprocess.Popen[str]:
    stdout_file = stdout_path.open("w")
    stderr_file = stderr_path.open("w")
    return subprocess.Popen(
        [str(server_path), str(port)],
        cwd=str(cwd),
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
    )


def read_log(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text()


def choose_port(port: int) -> int:
    if port != 0:
        return port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def stop_server(process: subprocess.Popen[str], *, announce: bool = True) -> None:
    if process.poll() is not None:
        return

    if announce:
        print("killing server...", flush=True)
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def run_roundtrip(server_path: Path, port: int, count: int, restart_wait: float, persist_mode: str) -> None:
    server_path = server_path.resolve()
    with tempfile.TemporaryDirectory(prefix="kvstore-aof-roundtrip-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        stdout_path = temp_dir / "server.out"
        stderr_path = temp_dir / "server.err"
        port = choose_port(port)
        process = start_server(server_path, port, temp_dir, stdout_path, stderr_path)
        try:
            client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, protocol=2)
            wait_for_server(client, restart_wait)

            if persist_mode == "aof":
                response = client.execute_command("APPENDONLY", "yes")
                if str(response).upper() != "OK":
                    raise RuntimeError(f"APPENDONLY yes failed: {response!r}")

            rng = random.Random(DEFAULT_SEED)
            expected: dict[str, str] = {}
            last_key = ""
            for index in range(count):
                key = f"item:{index:05d}"
                value = random_value(rng, index)
                expected[key] = value
                if not client.set(key, value):
                    raise RuntimeError(f"SET failed for {key}")
                last_key = key
                if (index + 1) % BATCH_SIZE == 0 or (index + 1) == count:
                    print(f"wrote {index + 1}/{count} key-value pairs", flush=True)

            if persist_mode == "rdb":
                response = client.execute_command("SAVE")
                if response not in (True, "OK", b"OK"):
                    raise RuntimeError(f"SAVE failed: {response!r}")

            stop_server(process)

            print("server killed, restarting...", flush=True)
            process = start_server(server_path, port, temp_dir, stdout_path, stderr_path)
            wait_for_server(client, restart_wait)
            print("server restored, validating data...", flush=True)

            mismatches: list[str] = []
            validated = 0
            for key, expected_value in expected.items():
                actual_value = client.get(key)
                if actual_value != expected_value:
                    mismatches.append(key)
                    if len(mismatches) >= 10:
                        break
                validated += 1
                if validated % BATCH_SIZE == 0 or validated == len(expected):
                    print(f"validated {validated}/{len(expected)} key-value pairs", flush=True)

            if mismatches:
                raise RuntimeError(f"restored values do not match for keys: {', '.join(mismatches)}")

            print(f"restored {len(expected)} key-value pairs successfully", flush=True)
        except Exception:
            if 'last_key' in locals() and last_key:
                print(f"last successful key: {last_key}")
            print("--- server stdout ---")
            print(read_log(stdout_path))
            print("--- server stderr ---")
            print(read_log(stderr_path))
            raise
        finally:
            stop_server(process, announce=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load 5,000 varying items into the server, restart it, and verify recovery.")
    parser.add_argument("--server", type=Path, default=DEFAULT_SERVER, help="Path to the server executable")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to run the server on")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of data items to write")
    parser.add_argument("--restart-wait", type=float, default=DEFAULT_RESTART_WAIT, help="Seconds to wait for the server to become ready")
    parser.add_argument("--persist", choices=("rdb", "aof"), default="rdb", help="Persistence mode to verify before restart")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.server.exists():
        print(f"server executable not found: {args.server}", file=sys.stderr)
        return 2

    run_roundtrip(args.server, args.port, args.count, args.restart_wait, args.persist)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())