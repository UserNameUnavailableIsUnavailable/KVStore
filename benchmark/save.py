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

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PACKAGE_ROOT))
from benchmark import PROJECT_ROOT

try:
    import redis
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    redis = None
    REDIS_IMPORT_ERROR = exc
else:
    REDIS_IMPORT_ERROR = None

TEMP_ROOT = PROJECT_ROOT/"temp"
DEFAULT_SERVER = Path(f"{PROJECT_ROOT}/build/Application/Server/Server")
DEFAULT_PORT = 0
DEFAULT_COUNT = 100_000
DEFAULT_RESTART_WAIT = 30.0
DEFAULT_SEED = 0x6B7673746F7265
BATCH_SIZE = 1000

def random_value(rng: random.Random, index: int) -> str:
    alphabet = string.ascii_letters + string.digits
    size = 1 + (index % 7) * 11 + rng.randrange(0, 64)
    return "".join(rng.choice(alphabet) for _ in range(size))


def require_redis() -> None:
    if redis is None:
        raise RuntimeError("This script requires the 'redis' Python package. Install it with 'pip install redis'.") from REDIS_IMPORT_ERROR


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


def start_server(server_path: Path, port: int, cwd: Path, stdout_path: Path, stderr_path: Path,
                 config_path: Path | None = None) -> subprocess.Popen[str]:
    stdout_file = stdout_path.open("w")
    stderr_file = stderr_path.open("w")
    command = [str(server_path), "--port", str(port)]
    if config_path is not None:
        command.extend(("--config", str(config_path)))
    return subprocess.Popen(
        command,
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


def run_roundtrip(server_path: Path, port: int, count: int, restart_wait: float, persist_mode: str, *,
                  keep_temp: bool, config_path: Path | None = None) -> None:
    require_redis()
    server_path = server_path.resolve()
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    cleanup_temp = False
    if keep_temp:
        temp_dir = Path(tempfile.mkdtemp(prefix="kvstore-aof-roundtrip-", dir=TEMP_ROOT))
    else:
        temp_dir_obj = tempfile.TemporaryDirectory(prefix="kvstore-aof-roundtrip-", dir=TEMP_ROOT)
        temp_dir = Path(temp_dir_obj.name)
        cleanup_temp = True

    print(f"temporary directory: {temp_dir}", flush=True)
    stdout_path = temp_dir / "server.out"
    stderr_path = temp_dir / "server.err"
    port = choose_port(port)
    process = start_server(server_path, port, temp_dir, stdout_path, stderr_path, config_path)
    try:
        client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, protocol=2)
        wait_for_server(client, restart_wait)

        if persist_mode == "aof":
            response = client.execute_command("CONFIG", "SET", "appendonly", "yes")
            if str(response).upper() != "OK":
                raise RuntimeError(f"CONFIG SET appendonly yes failed: {response!r}")

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
            # The snapshot command forks, so it is named the way Redis names a
            # fork-then-write.
            response = client.execute_command("BGSAVE")
            if str(response).upper() != "BACKGROUND SAVING STARTED":
                raise RuntimeError(f"BGSAVE failed: {response!r}")

        stop_server(process)

        print("server killed, restarting...", flush=True)
        process = start_server(server_path, port, temp_dir, stdout_path, stderr_path, config_path)
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
        if cleanup_temp:
            temp_dir_obj.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load 5,000 varying items into the server, restart it, and verify recovery.")
    parser.add_argument("--server", type=Path, default=DEFAULT_SERVER, help="Path to the server executable")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to run the server on")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of data items to write")
    parser.add_argument("--restart-wait", type=float, default=DEFAULT_RESTART_WAIT, help="Seconds to wait for the server to become ready")
    parser.add_argument("--persist", choices=("rdb", "aof"), default="rdb", help="Persistence mode to verify before restart")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary artifacts under PROJECT_ROOT/temp")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.server.exists():
        print(f"server executable not found: {args.server}", file=sys.stderr)
        return 2

    run_roundtrip(args.server, args.port, args.count, args.restart_wait, args.persist, keep_temp=args.keep_temp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())