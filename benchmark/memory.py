#!/usr/bin/env python3
"""Measure KVStore memory before, during, and after a write/delete cycle.

The benchmark starts a local KVStore server, records its initial memory usage,
then fans out several concurrent clients, repeats insert/remove cycles, and
records the peak and end memory for each cycle so RSS trends can be compared.

That makes it easy to answer the pooling question: does the server return near
its starting resident size after the KVs are gone, or does it keep growing?
"""

from __future__ import annotations

import argparse
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from benchmark import (  # noqa: E402
    DEFAULT_BUILD_DIRECTORY,
    DEFAULT_COUNT,
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_SEED,
    find_target_file,
    memory_of,
    records,
    keys,
)

DEFAULT_CONFIG = PROJECT_ROOT / "benchmark" / "memory.conf"
DEFAULT_STARTUP_TIMEOUT = 15.0
DEFAULT_SAMPLE_INTERVAL = 0.05
DEFAULT_CLIENTS = 4


@dataclass
class MemoryPoint:
    label: str
    rss_kb: int
    hwm_kb: int
    vsz_kb: int

    def rss_mib(self) -> float:
        return self.rss_kb / 1024.0

    def hwm_mib(self) -> float:
        return self.hwm_kb / 1024.0

    def vsz_mib(self) -> float:
        return self.vsz_kb / 1024.0


@dataclass
class PhaseResult:
    name: str
    commands: int
    elapsed_s: float
    samples: list[MemoryPoint]

    @property
    def peak_rss_kb(self) -> int:
        return max(sample.rss_kb for sample in self.samples)

    @property
    def peak_hwm_kb(self) -> int:
        return max(sample.hwm_kb for sample in self.samples)

    @property
    def rps(self) -> float:
        return self.commands / self.elapsed_s if self.elapsed_s > 0 else float("inf")


@dataclass
class WorkItem:
    index: int
    start: int
    count: int


class RespConnection:
    """Minimal RESP2 client for SET and DEL commands."""

    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.create_connection((host, port))
        self._reader = self._socket.makefile("rb")

    def close(self) -> None:
        try:
            self._reader.close()
        finally:
            self._socket.close()

    def __enter__(self) -> "RespConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def execute(self, *parts: str) -> object:
        payload = [f"*{len(parts)}\r\n".encode()]
        for part in parts:
            data = part.encode()
            payload.append(f"${len(data)}\r\n".encode())
            payload.append(data)
            payload.append(b"\r\n")
        self._socket.sendall(b"".join(payload))
        return self._read_reply()

    def _readline(self) -> bytes:
        line = self._reader.readline()
        if not line:
            raise RuntimeError("server closed the connection unexpectedly")
        return line.rstrip(b"\r\n")

    def _read_exact(self, length: int) -> bytes:
        data = self._reader.read(length)
        if data is None or len(data) != length:
            raise RuntimeError("server closed the connection while reading a bulk reply")
        suffix = self._reader.read(2)
        if suffix != b"\r\n":
            raise RuntimeError("RESP bulk reply was not terminated correctly")
        return data

    def _read_reply(self) -> object:
        prefix = self._reader.read(1)
        if not prefix:
            raise RuntimeError("server closed the connection unexpectedly")
        if prefix == b"+":
            return self._readline().decode(errors="replace")
        if prefix == b"-":
            raise RuntimeError(self._readline().decode(errors="replace"))
        if prefix == b":":
            return int(self._readline())
        if prefix == b"$":
            length = int(self._readline())
            if length < 0:
                return None
            return self._read_exact(length).decode(errors="replace")
        if prefix == b"*":
            count = int(self._readline())
            return [self._read_reply() for _ in range(count)]
        raise RuntimeError(f"unsupported RESP reply type: {prefix!r}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure initial, peak, and end server memory while inserting and removing KVs."
    )
    parser.add_argument("--target", default="Server", help="CMake target for the KVStore server executable")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIRECTORY, help="CMake build directory")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="KVStore startup configuration")
    parser.add_argument("--multiplexer", choices=("epoll", "io_uring"), default="epoll",
                        help="Server event multiplexer")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Server host to connect insert/remove scripts to")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port; 0 picks a free one")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Number of key-value pairs to insert/remove")
    parser.add_argument("--clients", type=int, default=DEFAULT_CLIENTS,
                        help="Number of concurrent clients to fan out")
    parser.add_argument("--cycles", type=int, default=2,
                        help="Number of insert/remove cycles to run")
    parser.add_argument("--start", type=int, default=0, help="First key number (item:0 by default)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Seed used to generate values")
    parser.add_argument("--sample-interval", type=float, default=DEFAULT_SAMPLE_INTERVAL,
                        help="Seconds between memory samples while a phase runs")
    parser.add_argument("--startup-timeout", type=float, default=DEFAULT_STARTUP_TIMEOUT,
                        help="Seconds to wait for the server to start listening")
    parser.add_argument("--keep-data", action="store_true", help="Keep the server's scratch directory")
    return parser.parse_args()


def choose_port(port: int) -> int:
    if port != 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(f"port {port} is already in use; stop that server first") from error
        return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


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


def snapshot(pid: int, label: str) -> MemoryPoint:
    memory = memory_of(pid)
    if memory is None:
        raise RuntimeError(f"could not read memory for pid {pid}")
    return MemoryPoint(label=label, rss_kb=memory.rss, hwm_kb=memory.peak, vsz_kb=memory.vsz)


def split_work(count: int, clients: int, start: int) -> list[WorkItem]:
    if clients <= 0:
        raise RuntimeError("clients must be positive")
    base, extra = divmod(count, clients)
    current = start
    work: list[WorkItem] = []
    for index in range(clients):
        chunk = base + (1 if index < extra else 0)
        work.append(WorkItem(index=index, start=current, count=chunk))
        current += chunk
    return work


def run_phase(
    workers: list[Callable[[threading.Barrier], None]], pid: int, name: str, interval: float, commands: int
) -> PhaseResult:
    if not workers:
        return PhaseResult(name=name, commands=commands, elapsed_s=0.0, samples=[snapshot(pid, name)])

    error: list[BaseException] = []
    barrier = threading.Barrier(len(workers) + 1)

    def runner(worker: Callable[[threading.Barrier], None]) -> None:
        try:
            worker(barrier)
        except BaseException as exc:  # noqa: BLE001
            error.append(exc)
            try:
                barrier.abort()
            except threading.BrokenBarrierError:
                pass

    threads = [threading.Thread(target=runner, args=(worker,), daemon=True) for worker in workers]
    for thread in threads:
        thread.start()

    try:
        barrier.wait()
    except threading.BrokenBarrierError as error_exc:
        for thread in threads:
            thread.join()
        if error:
            raise error[0]
        raise RuntimeError("one of the clients failed before the phase could start") from error_exc

    started = time.perf_counter()
    samples: list[MemoryPoint] = [snapshot(pid, name)]
    while any(thread.is_alive() for thread in threads):
        time.sleep(interval)
        samples.append(snapshot(pid, name))
    for thread in threads:
        thread.join()
    elapsed_s = time.perf_counter() - started
    if error:
        raise error[0]
    samples.append(snapshot(pid, f"{name}:done"))
    return PhaseResult(name=name, commands=commands, elapsed_s=elapsed_s, samples=samples)


def format_mib(kib: int) -> str:
    return f"{kib / 1024.0:.1f} MiB"


def print_point(label: str, point: MemoryPoint) -> None:
    print(
        f"{label}: rss {format_mib(point.rss_kb)}, peak {format_mib(point.hwm_kb)}, vsize {format_mib(point.vsz_kb)}",
        flush=True,
    )


def print_phase_rate(label: str, phase: PhaseResult) -> None:
    print(f"{label}: {phase.commands:,} commands in {phase.elapsed_s:.2f} s -> {phase.rps:,.0f} rps", flush=True)


def insert_worker(host: str, port: int, seed: int, work: WorkItem) -> Callable[[threading.Barrier], None]:
    def worker(barrier: threading.Barrier) -> None:
        with RespConnection(host, port) as client:
            barrier.wait()
            for key, value in records(seed, work.count, work.start):
                if client.execute("SET", key, value) != "OK":
                    raise RuntimeError(f"SET {key} failed")

    return worker


def remove_worker(host: str, port: int, work: WorkItem) -> Callable[[threading.Barrier], None]:
    def worker(barrier: threading.Barrier) -> None:
        with RespConnection(host, port) as client:
            barrier.wait()
            for key in keys(work.count, work.start):
                client.execute("DEL", key)

    return worker


def main() -> int:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("this benchmark reads /proc/<pid>/status, so it only runs on Linux")

    args = parse_args()
    if args.count < 0 or args.start < 0 or args.sample_interval <= 0 or args.clients <= 0 or args.cycles <= 0:
        raise RuntimeError(
            "count, start, clients, cycles, and sample-interval must be non-negative/positive as appropriate"
        )
    if not args.config.is_file():
        raise RuntimeError(f"configuration was not found: {args.config}")

    server = find_target_file(args.target, args.build_dir)
    port = choose_port(args.port)
    scratch_root = PROJECT_ROOT / "temp"
    scratch_root.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="kvstore-mem-pool-", dir=scratch_root))

    print(f"server: {server}")
    print(f"port: {port}")
    print(f"count: {args.count}")
    print(f"scratch: {data_dir}", flush=True)

    stdout_path = data_dir / "server.stdout.log"
    stderr_path = data_dir / "server.stderr.log"
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        process = subprocess.Popen(
            [str(server), "--port", str(port), "--multiplexer", args.multiplexer, "--config", str(args.config.resolve())],
            cwd=data_dir, stdout=stdout, stderr=stderr, text=True,
        )
    try:
        wait_for_port(port, process, args.startup_timeout)
        server_pid = process.pid
        initial = snapshot(server_pid, "initial")
        print_point("initial", initial)

        work_items = split_work(args.count, args.clients, args.start)

        cycle_results: list[tuple[int, MemoryPoint, MemoryPoint]] = []
        for cycle in range(1, args.cycles + 1):
            print(f"\n--- cycle {cycle}: SET {args.count} keys across {args.clients} clients", flush=True)
            insert_result = run_phase(
                [insert_worker(args.host, port, args.seed, work) for work in work_items],
                server_pid,
                f"insert:{cycle}",
                args.sample_interval,
                args.count,
            )
            insert_peak = max(insert_result.samples, key=lambda sample: sample.rss_kb)
            print_point(f"cycle {cycle} peak after insert", insert_peak)
            print_phase_rate(f"cycle {cycle} insert", insert_result)

            print(f"\n--- cycle {cycle}: DEL {args.count} keys across {args.clients} clients", flush=True)
            remove_result = run_phase(
                [remove_worker(args.host, port, work) for work in work_items],
                server_pid,
                f"remove:{cycle}",
                args.sample_interval,
                args.count,
            )
            after_remove = remove_result.samples[-1]
            print_point(f"cycle {cycle} after remove", after_remove)
            print_phase_rate(f"cycle {cycle} remove", remove_result)
            cycle_results.append((cycle, insert_peak, after_remove))

        final_cycle, final_insert_peak, end = cycle_results[-1][0], cycle_results[-1][1], cycle_results[-1][2]

        print(
            "\nsummary: "
            f"initial rss {format_mib(initial.rss_kb)}, "
            + ", ".join(
                (
                    f"cycle {cycle} peak rss {format_mib(insert_peak.rss_kb)} (HWM {format_mib(insert_peak.hwm_kb)}), "
                    f"cycle {cycle} end rss {format_mib(after_remove.rss_kb)}, "
                    f"cycle {cycle} retained {format_mib(after_remove.rss_kb - initial.rss_kb)}"
                )
                for cycle, insert_peak, after_remove in cycle_results
            ),
            flush=True,
        )
        return 0
    except Exception:
        print("--- server stdout ---", file=sys.stderr)
        print(stdout_path.read_text(errors="replace") if stdout_path.exists() else "", file=sys.stderr)
        print("--- server stderr ---", file=sys.stderr)
        print(stderr_path.read_text(errors="replace") if stderr_path.exists() else "", file=sys.stderr)
        raise
    finally:
        stop_server(process)
        if not args.keep_data:
            shutil.rmtree(data_dir, ignore_errors=True)
        else:
            print(f"kept scratch directory: {data_dir}", flush=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"memory pool benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
