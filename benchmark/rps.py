#!/usr/bin/env python3

from __future__ import annotations

import argparse
import ctypes
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark import DEFAULT_BUILD_DIRECTORY, PROJECT_ROOT, find_target_file

DEFAULT_CONFIG = PROJECT_ROOT / "benchmark" / "rps.conf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Redis and both KVStore multiplexers with labeled output.")
    parser.add_argument("--clients", type=int, default=100, help="Concurrent benchmark clients")
    parser.add_argument("--requests", type=int, default=1_000_000, help="Requests per benchmark")
    parser.add_argument("--pipeline-depths", type=int, nargs="+", default=(1, 16, 64), help="Pipeline depths to test")
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--kvstore-port", type=int, default=6666, help="KVStore port from the startup configuration")
    parser.add_argument("--target", default="Server", help="CMake target for the KVStore serv/lister executable")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIRECTORY, help="CMake build directory")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="KVStore startup configuration")
    parser.add_argument("--redis-benchmark", default="redis-benchmark", help="redis-benchmark executable")
    parser.add_argument("--startup-timeout", type=float, default=10.0, help="Seconds to wait for KVStore startup")
    return parser.parse_args()


def require_file(path: Path, description: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise RuntimeError(f"{description} was not found: {resolved}")
    return resolved


def listening_inodes(port: int) -> set[str]:
    """The socket inodes with a LISTEN socket on `port`, from /proc/net/tcp{,6}."""
    inodes: set[str] = set()
    for name in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = Path(name).read_text(errors="replace").splitlines()
        except OSError:
            continue
        for line in lines[1:]:  # the first line names the columns
            fields = line.split()
            if len(fields) < 10 or fields[3] != "0A":  # 0A is TCP_LISTEN
                continue
            try:
                local_port = int(fields[1].rsplit(":", 1)[1], 16)
            except (IndexError, ValueError):
                continue
            if local_port == port:
                inodes.add(fields[9])
    return inodes


def listening_pids(port: int) -> list[int]:
    """The pids with a listening socket on `port`.

    `ss` and `lsof` say this in one line, but neither is guaranteed to be
    installed and /proc always is -- and this only has to work where the server
    is benchmarked, which is Linux.
    """
    inodes = listening_inodes(port)
    if not inodes:
        return []
    pids: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            descriptors = list((entry / "fd").iterdir())
        except OSError:
            continue
        for descriptor in descriptors:
            try:
                target = os.readlink(descriptor)
            except OSError:
                continue
            if target.startswith("socket:[") and target[len("socket:["):-1] in inodes:
                pids.append(int(entry.name))
                break
    return pids


def is_this_kvstore(pid: int, server: Path) -> bool:
    try:
        return Path(f"/proc/{pid}/exe").resolve() == server.resolve()
    except OSError:
        pass
    # A process whose executable cannot be read (someone else's) still says what
    # it is on its command line.
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        return False
    return server.name in cmdline


def stop_existing_kvstore(server: Path, port: int, timeout: float = 10.0) -> None:
    """Stops the KVStore holding `port`, so this run can have it to itself.

    A run that is interrupted leaves its server behind, and the next run would
    otherwise refuse to start over the port that server took. Only this
    repository's KVStore binary is stopped: another program on the port is
    reported instead, because a benchmark has no business killing a server it
    did not start.
    """
    victims = listening_pids(port)
    unknown = [pid for pid in victims if not is_this_kvstore(pid, server)]
    if unknown:
        raise RuntimeError(f"port {port} is held by another program (pid {unknown[0]}); "
                           f"stop it, or benchmark on a different --kvstore-port")

    for pid in victims:
        print(f"stopping the KVStore already listening on port {port} (pid {pid})", flush=True)
        os.kill(pid, signal.SIGTERM)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not listening_pids(port):
            return
        time.sleep(0.05)

    for pid in listening_pids(port):
        if pid in victims:
            print(f"killing the KVStore on port {port} (pid {pid}): it did not stop", flush=True)
            os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and listening_pids(port):
        time.sleep(0.05)


def wait_for_port(port: int, process: subprocess.Popen[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"KVStore exited before listening (exit code {process.returncode})")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"KVStore did not listen on 127.0.0.1:{port} within {timeout:.1f} seconds")


def stop_server(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def server_log(path: Path) -> str:
    return path.read_text(errors="replace") if path.exists() else ""


def run_benchmark(executable: str, title: str, host: str, port: int, clients: int, requests: int, pipeline_depth: int) -> None:
    command = [executable, "-t", "set,get,del", "-c", str(clients), "-n", str(requests), "-q", "-h", host, "-p", str(port)]
    if pipeline_depth > 1:
        command.extend(("-P", str(pipeline_depth)))

    mode = "no pipeline" if pipeline_depth == 1 else f"pipeline P={pipeline_depth}"
    print(f"\n=== {title} | {mode} ===", flush=True)
    print(" ".join(command), flush=True)
    subprocess.run(command, check=True)


def die_with_parent() -> None:
    """Asks the kernel to end this child when the benchmark goes away.

    A benchmark that is killed -- Ctrl-C twice, a terminal closing -- otherwise
    leaves its server holding the port. This runs in the child after fork, so it
    must stay this small; and it cannot be used from a process with threads,
    which is why the caller only asks for it when there is one thread.
    """
    ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG


def run_kvstore_benchmarks(args: argparse.Namespace, server: Path, config: Path, backend: str) -> None:
    # The port is taken before anything is measured, so a server left behind by
    # an interrupted run cannot decide whether this one starts.
    stop_existing_kvstore(server, args.kvstore_port)
    with tempfile.TemporaryDirectory(prefix=f"kvstore-rps-{backend}-") as directory:
        log_directory = Path(directory)
        stdout_path = log_directory / "server.stdout.log"
        stderr_path = log_directory / "server.stderr.log"
        with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
            process = subprocess.Popen(
                # The port is named on the command line as well as in the
                # configuration: the command line is what the server obeys, so
                # the instance that comes up is the one this benchmark talks to.
                [str(server), "--port", str(args.kvstore_port), "-c", str(config), "--multiplexer", backend],
                cwd=log_directory,
                stdout=stdout,
                stderr=stderr,
                text=True,
                preexec_fn=die_with_parent if threading.active_count() == 1 else None,
            )
            try:
                wait_for_port(args.kvstore_port, process, args.startup_timeout)
                print(f"\nKVStore {backend} listening on 127.0.0.1:{args.kvstore_port} (pid {process.pid})", flush=True)
                for depth in args.pipeline_depths:
                    run_benchmark(args.redis_benchmark, f"KVStore ({backend} multiplexer)", "127.0.0.1",
                                  args.kvstore_port, args.clients, args.requests, depth)
            except Exception:
                stop_server(process)
                print("--- KVStore stdout ---", file=sys.stderr)
                print(server_log(stdout_path), file=sys.stderr)
                print("--- KVStore stderr ---", file=sys.stderr)
                print(server_log(stderr_path), file=sys.stderr)
                raise
            finally:
                stop_server(process)


def main() -> int:
    args = parse_args()
    if args.clients <= 0 or args.requests <= 0 or args.startup_timeout <= 0 or any(depth <= 0 for depth in args.pipeline_depths):
        raise RuntimeError("clients, requests, startup timeout, and pipeline depths must be positive")
    if shutil.which(args.redis_benchmark) is None:
        raise RuntimeError(f"redis-benchmark was not found: {args.redis_benchmark}")

    server = find_target_file(args.target, args.build_dir)
    config = require_file(args.config, "KVStore configuration")

    for depth in args.pipeline_depths:
        run_benchmark(args.redis_benchmark, "Redis", args.redis_host, args.redis_port, args.clients, args.requests, depth)

    for backend in ("epoll", "io_uring"):
        run_kvstore_benchmarks(args, server, config, backend)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error