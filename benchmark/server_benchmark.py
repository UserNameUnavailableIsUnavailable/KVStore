#!/usr/bin/env python3

import argparse
import json
import math
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


def encode_command(*parts: str) -> bytes:
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = part.encode()
        encoded.extend((f"${len(value)}\r\n".encode(), value, b"\r\n"))
    return b"".join(encoded)


def read_response(stream) -> None:
    marker = stream.read(1)
    if not marker:
        raise ConnectionError("server closed the connection")
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise ConnectionError("incomplete RESP response")
    if marker == b"-":
        raise RuntimeError(line[:-2].decode(errors="replace"))
    if marker in (b"+", b":"):
        return
    if marker == b"$":
        size = int(line[:-2])
        if size >= 0 and len(stream.read(size + 2)) != size + 2:
            raise ConnectionError("incomplete bulk response")
        return
    if marker == b"*":
        for _ in range(int(line[:-2])):
            read_response(stream)
        return
    raise RuntimeError(f"unknown RESP marker: {marker!r}")


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def process_memory_kib(pid: int) -> tuple[int, int]:
    rss = 0
    hwm = 0
    with open(f"/proc/{pid}/status", encoding="utf-8") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
            elif line.startswith("VmHWM:"):
                hwm = int(line.split()[1])
    return rss, hwm


def wait_for_server(process: subprocess.Popen, host: str, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"server exited with code {process.returncode}:\n{output}")
        try:
            with socket.create_connection((host, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(f"server did not listen on {host}:{port}")


def worker(host: str, port: int, request: bytes, warmup_end: float, finish: float,
           results: dict, index: int) -> None:
    completed = 0
    latencies = []
    error = None
    try:
        connection = socket.create_connection((host, port), timeout=5)
        connection.settimeout(5)
        stream = connection.makefile("rb")
        while time.monotonic() < finish:
            started = time.perf_counter_ns()
            connection.sendall(request)
            read_response(stream)
            ended = time.perf_counter_ns()
            if time.monotonic() >= warmup_end:
                completed += 1
                latencies.append((ended - started) / 1_000.0)
        stream.close()
        connection.close()
    except Exception as exception:
        error = str(exception)
    results[index] = (completed, latencies, error)


def run_benchmark(args) -> dict:
    executable = Path(args.server).absolute() if args.server else None
    if executable is None or not executable.is_file():
        raise FileNotFoundError(f"server executable not found: {executable}")

    persistent_directory = tempfile.mkdtemp(prefix="kvstore-benchmark-")
    if args.server_kind == "redis":
        command = [str(executable), "--bind", args.host, "--port", str(args.port),
                   "--save", "", "--appendonly", "no", "--dir", persistent_directory,
                   "--daemonize", "no", *args.server_arg]
    else:
        command = [str(executable), "--networking-model", args.networking_model,
                   "--cache-strategy", args.cache_strategy, "--port", str(args.port),
                   "--persistent-dir", persistent_directory, *args.server_arg]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, start_new_session=True)
    try:
        wait_for_server(process, args.host, args.port, args.startup_timeout)
        baseline_rss, _ = process_memory_kib(process.pid)

        request = encode_command("SET", args.key, args.value)
        started = time.monotonic()
        warmup_end = started + args.warmup
        finish = warmup_end + args.duration
        results = {}
        threads = [threading.Thread(target=worker,
                                    args=(args.host, args.port, request, warmup_end, finish, results, index))
                   for index in range(args.clients)]
        for thread in threads:
            thread.start()

        peak_rss = baseline_rss
        while any(thread.is_alive() for thread in threads):
            try:
                rss, hwm = process_memory_kib(process.pid)
                peak_rss = max(peak_rss, rss, hwm)
            except FileNotFoundError:
                break
            time.sleep(args.memory_interval)
        for thread in threads:
            thread.join()

        errors = [result[2] for result in results.values() if result[2]]
        if errors:
            raise RuntimeError("; ".join(errors))
        completed = sum(result[0] for result in results.values())
        latencies = [latency for result in results.values() for latency in result[1]]
        final_rss, hwm = process_memory_kib(process.pid)
        peak_rss = max(peak_rss, final_rss, hwm)
        return {
            "server_kind": args.server_kind,
            "networking_model": "redis" if args.server_kind == "redis" else args.networking_model,
            "cache_strategy": "redis" if args.server_kind == "redis" else args.cache_strategy,
            "value_size_bytes": len(args.value.encode()),
            "request_size_bytes": len(request),
            "clients": args.clients,
            "warmup_seconds": args.warmup,
            "duration_seconds": args.duration,
            "requests": completed,
            "qps": completed / args.duration,
            "request_throughput_mib_s": completed * len(request) / args.duration / (1024 * 1024),
            "latency_us": {
                "mean": statistics.fmean(latencies) if latencies else 0.0,
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
            },
            "memory_kib": {
                "baseline_rss": baseline_rss,
                "final_rss": final_rss,
                "peak_rss": peak_rss,
                "growth": final_rss - baseline_rss,
            },
            "server_command": command,
        }
    finally:
        if process.poll() is None:
            os.killpg(process.pid, 15)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, 9)
                process.wait()
        shutil.rmtree(persistent_directory, ignore_errors=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark KVStore QPS, latency, and server RSS")
    parser.add_argument("--server-kind", choices=("kvstore", "redis"), default="kvstore")
    parser.add_argument("--server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--networking-model", choices=("epoll", "io_uring"), default="epoll")
    parser.add_argument("--cache-strategy",
                        choices=("hash", "array", "red-black-tree", "skip-list"), default="hash")
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--memory-interval", type=float, default=0.05)
    parser.add_argument("--startup-timeout", type=float, default=5.0)
    parser.add_argument("--key", default="benchmark-key")
    value_group = parser.add_mutually_exclusive_group()
    value_group.add_argument("--value")
    value_group.add_argument("--value-size", type=int, default=64,
                             help="generate a value containing this many ASCII bytes")
    parser.add_argument("--server-arg", action="append", default=[],
                        help="extra server argument; repeat for multiple arguments")
    parser.add_argument("--json", action="store_true", help="emit JSON only")
    args = parser.parse_args()
    if args.clients <= 0 or args.duration <= 0 or args.warmup < 0 or args.memory_interval <= 0:
        parser.error("clients, duration, and memory interval must be positive; warmup cannot be negative")
    if args.value_size is not None and args.value_size <= 0:
        parser.error("value size must be positive")
    if args.value is None:
        args.value = "x" * args.value_size
    if args.server is None:
        args.server = "build/Server/KVServer" if args.server_kind == "kvstore" else "/usr/bin/redis-server"
    return args


def main() -> int:
    args = parse_args()
    try:
        result = run_benchmark(args)
    except Exception as exception:
        print(f"benchmark error: {exception}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2))
        return 0
    latency = result["latency_us"]
    memory = result["memory_kib"]
    print(f"{result['networking_model']} / {result['cache_strategy']} / "
          f"{result['value_size_bytes']} byte value / {result['clients']} clients")
    print(f"QPS: {result['qps']:,.0f} ({result['requests']:,} requests)")
    print(f"Request throughput: {result['request_throughput_mib_s']:,.2f} MiB/s")
    print(f"Latency us: mean {latency['mean']:,.1f}, p50 {latency['p50']:,.1f}, "
          f"p95 {latency['p95']:,.1f}, p99 {latency['p99']:,.1f}")
    print(f"RSS MiB: baseline {memory['baseline_rss'] / 1024:.2f}, "
          f"final {memory['final_rss'] / 1024:.2f}, peak {memory['peak_rss'] / 1024:.2f}, "
          f"growth {memory['growth'] / 1024:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())