#!/usr/bin/env python3

import argparse
import json
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Redis and compare it with KVStore")
    parser.add_argument("--redis-server", default="/usr/bin/redis-server")
    parser.add_argument("--kvstore-report", type=Path,
                        default=Path("benchmark/reports/server_matrix.json"))
    parser.add_argument("--value-sizes", type=int, nargs="+", default=(64, 4096, 65536))
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--warmup", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=3.0)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--output", type=Path,
                        default=Path("benchmark/reports/redis_comparison"))
    args = parser.parse_args()
    if any(size <= 0 for size in args.value_sizes):
        parser.error("value sizes must be positive")
    if args.clients <= 0 or args.duration <= 0 or args.warmup < 0 or args.repeat <= 0:
        parser.error("clients, duration, and repeat must be positive; warmup cannot be negative")
    return args


def run_case(args, value_size: int) -> dict:
    command = [
        sys.executable, str(Path(__file__).with_name("server_benchmark.py")),
        "--server-kind", "redis", "--server", args.redis_server,
        "--value-size", str(value_size), "--clients", str(args.clients),
        "--warmup", str(args.warmup), "--duration", str(args.duration),
        "--port", str(args.port), "--json",
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


def summarize(results: list[dict], value_size: int) -> dict:
    return {
        "value_size_bytes": value_size,
        "runs": len(results),
        "qps_mean": mean(results, "qps"),
        "qps_stddev": statistics.pstdev(result["qps"] for result in results),
        "request_throughput_mib_s_mean": mean(results, "request_throughput_mib_s"),
        "latency_p99_us_mean": mean(results, "latency_us", "p99"),
        "baseline_rss_mib_mean": mean(results, "memory_kib", "baseline_rss") / 1024,
        "peak_rss_mib_mean": mean(results, "memory_kib", "peak_rss") / 1024,
        "rss_growth_mib_mean": mean(results, "memory_kib", "growth") / 1024,
    }


def kvstore_average(kvstore_summary: list[dict], value_size: int) -> dict:
    cases = [item for item in kvstore_summary if item["value_size_bytes"] == value_size]
    return {key: statistics.fmean(item[key] for item in cases) for key in (
        "qps_mean", "request_throughput_mib_s_mean", "latency_p99_us_mean",
        "baseline_rss_mib_mean", "peak_rss_mib_mean", "rss_growth_mib_mean")}


def markdown(metadata: dict, redis_summary: list[dict], kvstore_summary: list[dict]) -> str:
    lines = [
        "# Redis Comparison Report", "", f"Generated: {metadata['generated_at']}", "",
        f"Redis: {metadata['redis_version']}", "",
        f"Clients: {metadata['clients']}; warmup: {metadata['warmup_seconds']} s; "
        f"measured duration: {metadata['duration_seconds']} s; repeats: {metadata['repeat']}", "",
        "Both servers use the same non-pipelined SET workload. Redis persistence is disabled. "
        "KVStore values are means across all eight backend/index combinations.", "",
        "| Value | Server | QPS | Request MiB/s | p99 us | Baseline RSS MiB | Peak RSS MiB | RSS growth MiB |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for redis in redis_summary:
        kvstore = kvstore_average(kvstore_summary, redis["value_size_bytes"])
        value = redis["value_size_bytes"]
        lines.append(f"| {value} B | KVStore mean | {kvstore['qps_mean']:.0f} | "
                     f"{kvstore['request_throughput_mib_s_mean']:.2f} | "
                     f"{kvstore['latency_p99_us_mean']:.1f} | "
                     f"{kvstore['baseline_rss_mib_mean']:.2f} | {kvstore['peak_rss_mib_mean']:.2f} | "
                     f"{kvstore['rss_growth_mib_mean']:.2f} |")
        lines.append(f"| {value} B | Redis | {redis['qps_mean']:.0f} +/- {redis['qps_stddev']:.0f} | "
                     f"{redis['request_throughput_mib_s_mean']:.2f} | "
                     f"{redis['latency_p99_us_mean']:.1f} | {redis['baseline_rss_mib_mean']:.2f} | "
                     f"{redis['peak_rss_mib_mean']:.2f} | {redis['rss_growth_mib_mean']:.2f} |")
        qps_delta = (redis["qps_mean"] / kvstore["qps_mean"] - 1) * 100
        lines.append(f"| {value} B | Redis vs KVStore | {qps_delta:+.2f}% | | | | | |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    kvstore_report = json.loads(args.kvstore_report.read_text(encoding="utf-8"))
    version = subprocess.run([args.redis_server, "--version"], check=True,
                             capture_output=True, text=True).stdout.strip()
    raw_results = []
    redis_summary = []
    total = len(args.value_sizes) * args.repeat
    current = 0
    for value_size in args.value_sizes:
        results = []
        for run in range(1, args.repeat + 1):
            current += 1
            print(f"[{current}/{total}] Redis / {value_size} B / run {run}",
                  file=sys.stderr, flush=True)
            result = run_case(args, value_size)
            results.append(result)
            raw_results.append(result)
        redis_summary.append(summarize(results, value_size))

    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "redis_version": version,
        "clients": args.clients, "warmup_seconds": args.warmup,
        "duration_seconds": args.duration, "repeat": args.repeat,
        "value_sizes_bytes": args.value_sizes,
    }
    report = {"metadata": metadata, "redis_summary": redis_summary,
              "kvstore_summary": kvstore_report["summary"], "redis_raw_results": raw_results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    json_path = args.output.with_suffix(".json")
    markdown_path = args.output.with_suffix(".md")
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(markdown(metadata, redis_summary, kvstore_report["summary"]),
                             encoding="utf-8")
    print(markdown_path)
    print(json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())