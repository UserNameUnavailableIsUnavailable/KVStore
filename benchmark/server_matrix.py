#!/usr/bin/env python3

import argparse
import json
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BACKENDS = ("epoll", "io_uring")
STORES = ("array", "hash", "red-black-tree", "skip-list")


def parse_args():
    parser = argparse.ArgumentParser(description="Run the KVStore backend/index benchmark matrix")
    parser.add_argument("--server", default="build/Server/KVServer")
    parser.add_argument("--allocators", nargs="+", choices=("default", "jemalloc", "pool"),
                        default=("default",))
    parser.add_argument("--value-sizes", type=int, nargs="+", default=(64, 4096, 65536))
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--output", type=Path,
                        default=Path("benchmark/reports/server_matrix"))
    args = parser.parse_args()
    if any(size <= 0 for size in args.value_sizes):
        parser.error("value sizes must be positive")
    if args.clients <= 0 or args.duration <= 0 or args.warmup < 0 or args.repeat <= 0:
        parser.error("clients, duration, and repeat must be positive; warmup cannot be negative")
    return args


def run_case(args, allocator: str, backend: str, store: str, value_size: int) -> dict:
    command = [
        sys.executable,
        str(Path(__file__).with_name("server_benchmark.py")),
        "--server", args.server,
        "--networking-model", backend,
        "--cache-strategy", store,
        "--value-size", str(value_size),
        "--clients", str(args.clients),
        "--warmup", str(args.warmup),
        "--duration", str(args.duration),
        "--port", str(args.port),
        "--server-arg=--allocator", f"--server-arg={allocator}",
        "--json",
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def mean(results: list[dict], *path: str) -> float:
    values = []
    for result in results:
        value = result
        for key in path:
            value = value[key]
        values.append(value)
    return statistics.fmean(values)


def aggregate(raw_results: list[dict]) -> list[dict]:
    groups = {}
    for result in raw_results:
        command = result["server_command"]
        allocator = command[command.index("--allocator") + 1]
        key = (allocator, result["networking_model"], result["cache_strategy"],
               result["value_size_bytes"])
        groups.setdefault(key, []).append(result)

    summaries = []
    for (allocator, backend, store, value_size), results in groups.items():
        summaries.append({
            "allocator": allocator,
            "networking_model": backend,
            "cache_strategy": store,
            "value_size_bytes": value_size,
            "runs": len(results),
            "qps_mean": mean(results, "qps"),
            "qps_stddev": statistics.pstdev(result["qps"] for result in results),
            "request_throughput_mib_s_mean": mean(results, "request_throughput_mib_s"),
            "latency_p99_us_mean": mean(results, "latency_us", "p99"),
            "baseline_rss_mib_mean": mean(results, "memory_kib", "baseline_rss") / 1024,
            "peak_rss_mib_mean": mean(results, "memory_kib", "peak_rss") / 1024,
            "rss_growth_mib_mean": mean(results, "memory_kib", "growth") / 1024,
        })
    return sorted(summaries, key=lambda item: (
        item["value_size_bytes"], item["cache_strategy"], item["networking_model"],
        item["allocator"]))


def backend_delta(summaries: list[dict], allocator: str, store: str, value_size: int) -> float:
    qps = {item["networking_model"]: item["qps_mean"] for item in summaries
        if item["allocator"] == allocator and item["cache_strategy"] == store
        and item["value_size_bytes"] == value_size}
    return (qps["io_uring"] / qps["epoll"] - 1) * 100


def allocator_delta(summaries: list[dict], backend: str, store: str, value_size: int) -> float:
    qps = {item["allocator"]: item["qps_mean"] for item in summaries
        if item["networking_model"] == backend and item["cache_strategy"] == store
        and item["value_size_bytes"] == value_size}
    return (qps["jemalloc"] / qps["default"] - 1) * 100


def markdown_report(metadata: dict, summaries: list[dict]) -> str:
    lines = [
        "# KVStore Server Benchmark Report",
        "",
        f"Generated: {metadata['generated_at']}",
        "",
        f"Clients: {metadata['clients']}; warmup: {metadata['warmup_seconds']} s; "
        f"measured duration: {metadata['duration_seconds']} s; repeats: {metadata['repeat']}",
        "",
        "Peak RSS is total resident memory for the server process, including the index, values, "
        "networking state, allocator overhead, and executable pages.",
        "",
        "| Value | Store | Backend | Allocator | QPS mean +/- sd | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summaries:
        lines.append(
            f"| {item['value_size_bytes']} B | {item['cache_strategy']} | "
            f"{item['networking_model']} | {item['allocator']} | "
            f"{item['qps_mean']:.0f} +/- {item['qps_stddev']:.0f} | "
            f"{item['request_throughput_mib_s_mean']:.2f} | "
            f"{item['latency_p99_us_mean']:.1f} | {item['baseline_rss_mib_mean']:.2f} | "
            f"{item['peak_rss_mib_mean']:.2f} | {item['rss_growth_mib_mean']:.2f} |")

    lines.extend([
        "",
        "## Backend Difference",
        "",
        "Positive values mean io_uring achieved higher QPS than epoll.",
        "",
        "| Value | Store | io_uring QPS delta |",
        "|---:|---|---:|",
    ])
    for allocator in metadata["allocators"]:
        for value_size in metadata["value_sizes_bytes"]:
            for store in STORES:
                lines.append(f"| {value_size} B / {allocator} | {store} | "
                             f"{backend_delta(summaries, allocator, store, value_size):+.2f}% |")
            average_delta = statistics.fmean(
                backend_delta(summaries, allocator, store, value_size) for store in STORES)
            lines.append(f"| **{value_size} B / {allocator}** | **mean across stores** | "
                         f"**{average_delta:+.2f}%** |")
    if "default" in metadata["allocators"] and "jemalloc" in metadata["allocators"]:
        lines.extend([
            "", "## Allocator Difference", "",
            "Positive values mean jemalloc achieved higher QPS than the default allocator.", "",
            "| Value | Store | Backend | jemalloc QPS delta |", "|---:|---|---|---:|",
        ])
        for value_size in metadata["value_sizes_bytes"]:
            deltas = []
            for store in STORES:
                for backend in BACKENDS:
                    delta = allocator_delta(summaries, backend, store, value_size)
                    deltas.append(delta)
                    lines.append(f"| {value_size} B | {store} | {backend} | {delta:+.2f}% |")
            lines.append(f"| **{value_size} B** | **mean across stores/backends** | **all** | "
                         f"**{statistics.fmean(deltas):+.2f}%** |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    raw_results = []
    total = len(args.allocators) * len(BACKENDS) * len(STORES) * len(args.value_sizes) * args.repeat
    current = 0
    for value_size in args.value_sizes:
        for store in STORES:
            for backend in BACKENDS:
                for allocator in args.allocators:
                    for run in range(1, args.repeat + 1):
                        current += 1
                        print(f"[{current}/{total}] {backend} / {store} / {allocator} / "
                              f"{value_size} B / run {run}", file=sys.stderr, flush=True)
                        raw_results.append(run_case(args, allocator, backend, store, value_size))

    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "server": str(Path(args.server).resolve()),
        "clients": args.clients,
        "warmup_seconds": args.warmup,
        "duration_seconds": args.duration,
        "repeat": args.repeat,
        "allocators": args.allocators,
        "value_sizes_bytes": args.value_sizes,
    }
    summaries = aggregate(raw_results)
    report = {"metadata": metadata, "summary": summaries, "raw_results": raw_results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    json_path = args.output.with_suffix(".json")
    markdown_path = args.output.with_suffix(".md")
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(markdown_report(metadata, summaries), encoding="utf-8")
    print(markdown_path)
    print(json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())