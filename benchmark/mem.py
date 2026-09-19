#!/usr/bin/env python3
"""Watch what a KVStore server's memory does while the load arrives in bursts.

The server is started on its own port in a scratch directory, redis-benchmark is
run against it `--bursts` times with quiet gaps in between, and
`/proc/<pid>/status` is sampled the whole time. The samples become a CSV and a
chart, so "does the memory come back down when the traffic stops?" is answered by
a picture rather than by a feeling.

A bounded keyspace matters here. redis-benchmark's SET with `-r` reuses keys, so
the store's own growth is bounded and the chart shows the allocator's behaviour
-- scratch buffers, the frame pool, jemalloc's arenas -- rather than a store that
is simply being filled. Growth that survives the quiet gaps is the part worth
looking at; growth that returns to the baseline is the cache working.

The chart is drawn with matplotlib when it is installed -- a PNG and a vector SVG
-- and with a small built-in SVG renderer when it is not, so the benchmark runs
under an interpreter that has no packages at all. `--renderer` picks one
explicitly. Reading /proc is Linux-only.
"""

from __future__ import annotations

import argparse
import csv
import html
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from benchmark import DEFAULT_BUILD_DIRECTORY, find_target_file

DEFAULT_CONFIG = PROJECT_ROOT / "benchmark" / "mem.conf"
DEFAULT_BURSTS = 16
DEFAULT_CLIENTS = 1000
DEFAULT_REQUESTS = 2_000_000
DEFAULT_KEYSPACE = 1_000_000
DEFAULT_INTERVAL = 0.1
DEFAULT_BASELINE = 2.0
DEFAULT_IDLE = 3.0
DEFAULT_STARTUP_TIMEOUT = 15.0

# The fields of /proc/<pid>/status that say what the process is holding. KiB
# except for the thread count, which is a plain count.
STATUS_FIELDS = {
    "VmRSS": "vm_rss_kb",
    "VmHWM": "vm_hwm_kb",
    "VmSize": "vm_size_kb",
    "RssAnon": "rss_anon_kb",
    "RssFile": "rss_file_kb",
    "Threads": "threads",
}
FIELDS = tuple(STATUS_FIELDS.values())


@dataclass
class Sample:
    elapsed: float
    phase: str
    burst: int
    values: dict[str, float]


@dataclass
class Burst:
    index: int
    start: float
    end: float
    output: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chart KVStore memory use across bursts of benchmark load.")
    parser.add_argument("--target", default="Server", help="CMake target for the KVStore server executable")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIRECTORY, help="CMake build directory")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="KVStore startup configuration")
    parser.add_argument("--multiplexer", choices=("epoll", "io_uring"), default="epoll", help="Server event multiplexer")
    parser.add_argument("--port", type=int, default=0, help="Server port; 0 picks a free one")
    parser.add_argument("--bursts", type=int, default=DEFAULT_BURSTS, help="Number of load bursts")
    parser.add_argument("--clients", type=int, default=DEFAULT_CLIENTS, help="Concurrent benchmark clients")
    parser.add_argument("--requests", type=int, default=DEFAULT_REQUESTS, help="Requests per burst")
    parser.add_argument("--keyspace", type=int, default=DEFAULT_KEYSPACE, help="Key space redis-benchmark reuses (-r)")
    parser.add_argument("--tests", default="set", help="redis-benchmark tests, e.g. set or get,set")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="Sampling interval in seconds")
    parser.add_argument("--baseline", type=float, default=DEFAULT_BASELINE, help="Seconds to sample before the first burst")
    parser.add_argument("--idle", type=float, default=DEFAULT_IDLE, help="Seconds to sample between bursts")
    parser.add_argument("--startup-timeout", type=float, default=DEFAULT_STARTUP_TIMEOUT, help="Seconds to wait for startup")
    parser.add_argument("--redis-benchmark", default="redis-benchmark", help="redis-benchmark executable")
    parser.add_argument("--renderer", choices=("auto", "matplotlib", "svg"), default="auto",
                        help="Chart renderer: matplotlib (PNG + SVG), svg (built-in, no dependencies), or auto")
    parser.add_argument("--output-dir", type=Path, default=None, help="Where the CSV and chart are written")
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


def read_status(path: Path) -> dict[str, float] | None:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    values: dict[str, float] = {}
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        field = STATUS_FIELDS.get(name.strip())
        if field is None:
            continue
        token = rest.split()
        if token:
            try:
                values[field] = float(token[0])
            except ValueError:
                continue
    return values


class Sampler(threading.Thread):
    """Samples /proc/<pid>/status until told to stop, labelling each sample."""

    def __init__(self, status_path: Path, interval: float) -> None:
        super().__init__(daemon=True)
        self.status_path = status_path
        self.interval = interval
        self.samples: list[Sample] = []
        self.phase = "startup"
        self.burst = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._origin = time.monotonic()

    def run(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                self._sample_locked()
            self._stop.wait(self.interval)

    def elapsed(self) -> float:
        return time.monotonic() - self._origin

    def mark(self, phase: str, burst: int = 0) -> None:
        # The phase change is itself worth a sample: the boundary between "under
        # load" and "quiet" is where the chart has something to show.
        with self._lock:
            self.phase = phase
            self.burst = burst
            self._sample_locked()

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=2.0)

    def _sample_locked(self) -> None:
        values = read_status(self.status_path)
        if values is None:
            return
        self.samples.append(Sample(self.elapsed(), self.phase, self.burst, values))


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


def run_burst(args: argparse.Namespace, port: int, index: int) -> list[str]:
    command = [args.redis_benchmark, "-t", args.tests, "-c", str(args.clients), "-n", str(args.requests), "-q",
               "-h", "127.0.0.1", "-p", str(port)]
    if args.keyspace > 0:
        command.extend(("-r", str(args.keyspace)))
    print(f"\n--- burst {index}: {' '.join(command)}", flush=True)
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    # redis-benchmark's quieter mode still prints its running samples; the line
    # that says what the burst achieved is the one worth keeping.
    lines = [line.rstrip() for line in result.stdout.splitlines() if "requests per second" in line]
    for line in lines:
        print(f"    {line}", flush=True)
    return lines


def mib(sample: Sample, field: str) -> float:
    return sample.values.get(field, 0.0) / 1024.0


def write_csv(path: Path, samples: list[Sample]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("elapsed_s", "phase", "burst", *FIELDS))
        for sample in samples:
            writer.writerow((f"{sample.elapsed:.3f}", sample.phase, sample.burst,
                             *[f"{sample.values[field]:.0f}" if field in sample.values else "" for field in FIELDS]))


CHART_WIDTH = 1100
CHART_HEIGHT = 560
PLOT_LEFT = 95
PLOT_RIGHT = 35
PLOT_TOP = 60
PLOT_BOTTOM = 75


def render_matplotlib(png_path: Path, svg_path: Path, samples: list[Sample], bursts: list[Burst], title: str) -> bool:
    """Draws the chart with matplotlib, or reports that it is not installed.

    Two panels sharing one time axis: what the process holds in total, and what
    that total is made of. Resident memory that is anonymous is the heap this
    process allocated; the file-backed part is the binary and its mappings, which
    is why a leak and a page-cache effect look different here.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    times = [sample.elapsed for sample in samples]
    rss = [mib(sample, "vm_rss_kb") for sample in samples]
    hwm = [mib(sample, "vm_hwm_kb") for sample in samples]
    anon = [mib(sample, "rss_anon_kb") for sample in samples]
    file_backed = [mib(sample, "rss_file_kb") for sample in samples]
    baseline = rss[0]

    figure, (top, bottom) = plt.subplots(2, 1, figsize=(12.0, 7.5), sharex=True,
                                         gridspec_kw={"height_ratios": (2, 1)})
    for axes in (top, bottom):
        axes.grid(True, alpha=0.25)
        for burst in bursts:
            axes.axvspan(burst.start, burst.end, color="#4c78a8", alpha=0.10, linewidth=0)

    for burst in bursts:
        middle = (burst.start + burst.end) / 2
        top.annotate(f"burst {burst.index}", xy=(middle, 1.0), xycoords=("data", "axes fraction"),
                     xytext=(0, 6), textcoords="offset points", ha="center", fontsize=9, color="#4c78a8")

    top.plot(times, rss, color="#2f6fbf", linewidth=1.6, label="VmRSS")
    top.plot(times, hwm, color="#f58518", linewidth=1.4, linestyle="--", label="VmHWM (peak)")
    top.axhline(baseline, color="#888888", linewidth=1.0, linestyle=":", label="baseline (first sample)")
    top.set_ylabel("MiB")
    # The padding is not decoration: the burst labels are drawn just above the
    # axes, and the title has to clear them.
    top.set_title(title, pad=24)
    top.legend(loc="lower right", fontsize=9, framealpha=0.9)

    bottom.plot(times, anon, color="#54a24b", linewidth=1.3, label="RssAnon (heap)")
    bottom.plot(times, file_backed, color="#e45756", linewidth=1.3, label="RssFile (mapped)")
    bottom.set_xlabel("seconds")
    bottom.set_ylabel("MiB")
    bottom.legend(loc="upper right", fontsize=9, framealpha=0.9)

    figure.tight_layout()
    figure.savefig(png_path, dpi=150)
    figure.savefig(svg_path)
    plt.close(figure)
    return True


def render_svg(path: Path, samples: list[Sample], bursts: list[Burst], title: str) -> None:
    """Draws the RSS line, the high-water mark, and where each burst ran."""
    if not samples:
        raise RuntimeError("no samples were collected, so there is nothing to chart")

    def mib(field: str, sample: Sample) -> float:
        return sample.values.get(field, 0.0) / 1024.0

    times = [sample.elapsed for sample in samples]
    rss = [mib("vm_rss_kb", sample) for sample in samples]
    hwm = [mib("vm_hwm_kb", sample) for sample in samples]

    t_end = max(times) if max(times) > 0 else 1.0
    y_low = min(rss)
    y_high = max(max(hwm), max(rss))
    if y_high - y_low < 1.0:
        y_high = y_low + 1.0
    padding = (y_high - y_low) * 0.08
    y_low = max(0.0, y_low - padding)
    y_high += padding

    plot_w = CHART_WIDTH - PLOT_LEFT - PLOT_RIGHT
    plot_h = CHART_HEIGHT - PLOT_TOP - PLOT_BOTTOM

    def x(time_point: float) -> float:
        return PLOT_LEFT + (time_point / t_end) * plot_w

    def y(value: float) -> float:
        return PLOT_TOP + (y_high - value) / (y_high - y_low) * plot_h

    def points(values: list[float]) -> str:
        return " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in zip(times, values))

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{CHART_WIDTH}" height="{CHART_HEIGHT}" '
        f'viewBox="0 0 {CHART_WIDTH} {CHART_HEIGHT}" font-family="sans-serif">',
        f'<rect width="{CHART_WIDTH}" height="{CHART_HEIGHT}" fill="#ffffff"/>',
        f'<text x="{CHART_WIDTH / 2:.0f}" y="30" text-anchor="middle" font-size="18" fill="#111">{html.escape(title)}</text>',
    ]

    # The bursts are drawn behind the data: what they say is where the load was.
    for burst in bursts:
        left = x(burst.start)
        right = x(burst.end)
        parts.append(f'<rect x="{left:.1f}" y="{PLOT_TOP}" width="{max(right - left, 0.5):.1f}" '
                     f'height="{plot_h}" fill="#4c78a8" fill-opacity="0.10"/>')
        parts.append(f'<text x="{(left + right) / 2:.1f}" y="{PLOT_TOP - 8}" text-anchor="middle" '
                     f'font-size="11" fill="#4c78a8">{burst.index}</text>')

    # Gridlines and their numbers, in MiB.
    for step in range(5):
        value = y_low + (y_high - y_low) * step / 4
        line_y = y(value)
        parts.append(f'<line x1="{PLOT_LEFT}" y1="{line_y:.1f}" x2="{PLOT_LEFT + plot_w}" y2="{line_y:.1f}" '
                     f'stroke="#e5e5e5" stroke-width="1"/>')
        parts.append(f'<text x="{PLOT_LEFT - 10}" y="{line_y + 4:.1f}" text-anchor="end" font-size="11" '
                     f'fill="#555">{value:.1f}</text>')

    for step in range(6):
        moment = t_end * step / 5
        tick_x = x(moment)
        parts.append(f'<line x1="{tick_x:.1f}" y1="{PLOT_TOP + plot_h}" x2="{tick_x:.1f}" '
                     f'y2="{PLOT_TOP + plot_h + 5}" stroke="#555" stroke-width="1"/>')
        parts.append(f'<text x="{tick_x:.1f}" y="{PLOT_TOP + plot_h + 20}" text-anchor="middle" font-size="11" '
                     f'fill="#555">{moment:.0f}</text>')

    parts.append(f'<line x1="{PLOT_LEFT}" y1="{PLOT_TOP}" x2="{PLOT_LEFT}" y2="{PLOT_TOP + plot_h}" '
                 f'stroke="#555" stroke-width="1"/>')
    parts.append(f'<line x1="{PLOT_LEFT}" y1="{PLOT_TOP + plot_h}" x2="{PLOT_LEFT + plot_w}" '
                 f'y2="{PLOT_TOP + plot_h}" stroke="#555" stroke-width="1"/>')

    parts.append(f'<polyline points="{points(hwm)}" fill="none" stroke="#f58518" stroke-width="1.5" '
                 f'stroke-dasharray="6 4"/>')
    parts.append(f'<polyline points="{points(rss)}" fill="none" stroke="#2f6fbf" stroke-width="1.8"/>')

    legend_x = PLOT_LEFT + 12
    legend_y = PLOT_TOP + 16
    parts.append(f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 26}" y2="{legend_y}" '
                 f'stroke="#2f6fbf" stroke-width="1.8"/>')
    parts.append(f'<text x="{legend_x + 32}" y="{legend_y + 4}" font-size="12" fill="#333">VmRSS</text>')
    parts.append(f'<line x1="{legend_x + 100}" y1="{legend_y}" x2="{legend_x + 126}" y2="{legend_y}" '
                 f'stroke="#f58518" stroke-width="1.5" stroke-dasharray="6 4"/>')
    parts.append(f'<text x="{legend_x + 132}" y="{legend_y + 4}" font-size="12" fill="#333">VmHWM (peak)</text>')
    parts.append(f'<rect x="{legend_x + 240}" y="{legend_y - 5}" width="18" height="10" fill="#4c78a8" '
                 f'fill-opacity="0.10"/>')
    parts.append(f'<text x="{legend_x + 264}" y="{legend_y + 4}" font-size="12" fill="#333">burst under load</text>')

    parts.append(f'<text x="{PLOT_LEFT + plot_w / 2:.0f}" y="{CHART_HEIGHT - 22}" text-anchor="middle" '
                 f'font-size="13" fill="#333">seconds</text>')
    parts.append(f'<text x="24" y="{PLOT_TOP + plot_h / 2:.0f}" text-anchor="middle" font-size="13" fill="#333" '
                 f'transform="rotate(-90 24 {PLOT_TOP + plot_h / 2:.0f})">MiB</text>')
    parts.append("</svg>\n")

    path.write_text("\n".join(parts))


def report(samples: list[Sample], bursts: list[Burst]) -> None:
    def rss(sample: Sample) -> float:
        return sample.values.get("vm_rss_kb", 0.0) / 1024.0

    if not samples:
        return
    print("\n=== memory ===", flush=True)
    print(f"baseline RSS {rss(samples[0]):.1f} MiB, peak RSS {max(rss(s) for s in samples):.1f} MiB, "
          f"high-water {max(s.values.get('vm_hwm_kb', 0.0) for s in samples) / 1024.0:.1f} MiB", flush=True)
    for burst in bursts:
        window = [sample for sample in samples if burst.start <= sample.elapsed <= burst.end]
        if not window:
            continue
        # What the quiet gap after this burst left behind, not what the last
        # sample of the whole run happens to be.
        quiet = [sample for sample in samples if sample.phase == "idle" and sample.burst == burst.index]
        settled = rss(quiet[-1]) if quiet else rss(window[-1])
        print(f"burst {burst.index}: peak RSS {max(rss(s) for s in window):.1f} MiB, "
              f"settled at {settled:.1f} MiB", flush=True)


def main() -> int:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("this benchmark reads /proc/<pid>/status, so it only runs on Linux")

    args = parse_args()
    if args.bursts <= 0 or args.clients <= 0 or args.requests <= 0 or args.interval <= 0:
        raise RuntimeError("bursts, clients, requests, and the sampling interval must all be positive")
    if shutil.which(args.redis_benchmark) is None:
        raise RuntimeError(f"redis-benchmark was not found: {args.redis_benchmark}")
    if not args.config.is_file():
        raise RuntimeError(f"configuration was not found: {args.config}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or (PROJECT_ROOT / "temp" / f"mem-{stamp}")
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    server = find_target_file(args.target, args.build_dir)
    port = choose_port(args.port)
    scratch_root = PROJECT_ROOT / "temp"
    scratch_root.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="kvstore-mem-", dir=scratch_root))
    stdout_path = data_dir / "server.stdout.log"
    stderr_path = data_dir / "server.stderr.log"

    print(f"server: {server}")
    print(f"port: {port}")
    print(f"output: {output_dir}")
    print(f"scratch: {data_dir}", flush=True)

    samples: list[Sample] = []
    bursts: list[Burst] = []
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        process = subprocess.Popen(
            [str(server), "--port", str(port), "--multiplexer", args.multiplexer, "--config", str(args.config.resolve())],
            cwd=data_dir, stdout=stdout, stderr=stderr, text=True,
        )
    try:
        wait_for_port(port, process, args.startup_timeout)
        sampler = Sampler(Path(f"/proc/{process.pid}/status"), args.interval)
        sampler.start()
        try:
            sampler.mark("baseline")
            print(f"\n--- baseline: {args.baseline:.1f}s of quiet", flush=True)
            time.sleep(args.baseline)

            for index in range(1, args.bursts + 1):
                sampler.mark("burst", index)
                start = sampler.elapsed()
                output = run_burst(args, port, index)
                end = sampler.elapsed()
                bursts.append(Burst(index, start, end, output))
                sampler.mark("idle", index)
                print(f"--- idle: {args.idle:.1f}s", flush=True)
                time.sleep(args.idle)
        finally:
            sampler.stop()
            samples = sampler.samples
    except Exception:
        stop_server(process)
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

    csv_path = output_dir / "mem-samples.csv"
    png_path = output_dir / "mem-usage.png"
    svg_path = output_dir / "mem-usage.svg"
    title = (f"KVStore memory under {args.bursts} bursts of redis-benchmark "
             f"({args.clients} clients, {args.requests} requests, {args.multiplexer})")
    write_csv(csv_path, samples)
    drawn = args.renderer in ("auto", "matplotlib") and render_matplotlib(png_path, svg_path, samples, bursts, title)
    if drawn:
        charts = f"{png_path} (+ .svg)"
    elif args.renderer == "matplotlib":
        raise RuntimeError("matplotlib was requested but is not installed")
    else:
        render_svg(svg_path, samples, bursts, title)
        charts = f"{svg_path} (built-in renderer)"
    report(samples, bursts)
    print(f"\nsamples: {csv_path}\nchart:   {charts}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"memory benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
