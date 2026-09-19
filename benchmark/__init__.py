#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import random
import re
import string
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUILD_DIRECTORY = PROJECT_ROOT / "build"
TARGET_FILE_PATTERN = re.compile(r"^TARGET_FILE=(.+)$", re.MULTILINE)
FIND_TARGET_PATTERN = re.compile(r"^FIND_TARGET:[^=]*=(.*)$", re.MULTILINE)

# The records a check works from: one script writes them into a server, another
# reads them back and compares. They are generated from a seed rather than kept,
# so the two scripts have to agree on nothing but the two numbers.
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6666
DEFAULT_COUNT = 100_000
DEFAULT_SEED = 0x6B7673746F7265
PROGRESS_EVERY = 1000


def random_value(rng: random.Random, index: int) -> str:
    alphabet = string.ascii_letters + string.digits
    size = 1 + (index % 7) * 11 + rng.randrange(0, 64)
    return "".join(rng.choice(alphabet) for _ in range(size))


def keys(count: int) -> Iterator[str]:
    """The keys a run is about, without the values: removing them does not care
    what they hold."""
    for index in range(count):
        yield f"item:{index:05d}"


def records(seed: int, count: int) -> Iterator[tuple[str, str]]:
    """The `count` key/value pairs a run is about, in the order they are
    written. The rng walks the indexes in order, so the same seed and count give
    the same pairs wherever they are generated."""
    rng = random.Random(seed)
    for index, key in enumerate(keys(count)):
        yield key, random_value(rng, index)


# How fast a run went, and what the server held while it did it. The server's
# numbers are in /proc -- that is where a process says how much memory it has --
# and the process is the one listening on the port being talked to.
@dataclass
class Memory:
    """What a process is holding, in KiB, as /proc/<pid>/status reports it."""

    rss: int
    peak: int
    vsz: int


def _listening_inodes(port: int) -> set[str]:
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


def server_pid_of(port: int) -> int | None:
    """The pid listening on `port`, or nothing when no local process does -- which
    is what a server on another host looks like. `ss` says this in one line, but it
    is not guaranteed to be installed and /proc always is."""
    inodes = _listening_inodes(port)
    if not inodes:
        return None
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            descriptors = list((entry / "fd").iterdir())
        except OSError:  # gone, or not ours to look at
            continue
        for descriptor in descriptors:
            try:
                target = os.readlink(descriptor)
            except OSError:
                continue
            if target.startswith("socket:[") and target[len("socket:["):-1] in inodes:
                return int(entry.name)
    return None


def memory_of(pid: int) -> Memory | None:
    """VmRSS (physical), VmHWM (its high-water mark) and VmSize (virtual): what a
    process says about itself, in KiB, in /proc/<pid>/status."""
    held: dict[str, int] = {}
    try:
        lines = Path(f"/proc/{pid}/status").read_text(errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        name, _, value = line.partition(":")
        if name in ("VmRSS", "VmHWM", "VmSize"):
            try:
                held[name] = int(value.split()[0])
            except (IndexError, ValueError):
                return None
    if len(held) != 3:
        return None
    return Memory(rss=held["VmRSS"], peak=held["VmHWM"], vsz=held["VmSize"])


def _size(kib: int) -> str:
    """A size in the unit that reads best."""
    if kib >= 1024 * 1024:
        return f"{kib / (1024 * 1024):.2f} GiB"
    if kib >= 1024:
        return f"{kib / 1024:.1f} MiB"
    return f"{kib} KiB"


def progress(verb: str, done: int, total: int) -> None:
    """The count so far, on one line that is rewritten in place.

    A run of millions of records must not fill the terminal with a line per
    thousand, so this only ever occupies the line it is on; the caller ends it
    with a newline when the run is over."""
    print(f"\r{verb} {done}/{total}", end="", flush=True)


class Meter:
    """How fast a run went and what the server held while it went.

    RPS is counted around the run: these scripts issue their commands one at a
    time and wait for each answer, so it is a round-trip rate and not a pipelined
    one. Memory is read from the server process before the first command and after
    the last, so what it reports is what the run cost it.
    """

    def __init__(self, port: int, server_pid: int | None = None) -> None:
        self.pid = server_pid if server_pid is not None else server_pid_of(port)
        self.started = time.perf_counter()
        self.before = memory_of(self.pid) if self.pid is not None else None

    def report(self, commands: int) -> None:
        """One line with everything the run measured, printed when it is over."""
        elapsed = time.perf_counter() - self.started
        rate = commands / elapsed if elapsed > 0 else float("inf")
        parts = [f"{elapsed:.2f} s elapsed", f"{rate:,.0f} rps"]

        if self.pid is None:
            parts.append("server memory: no local process listens on that port")
        else:
            after = memory_of(self.pid)
            if self.before is None or after is None:
                parts.append(f"server memory: unreadable for pid {self.pid}")
            else:
                parts.append(f"rss {_size(self.before.rss)} -> {_size(after.rss)} (peak {_size(after.peak)})")
                parts.append(f"vsz {_size(self.before.vsz)} -> {_size(after.vsz)}")

        print("stats: " + ", ".join(parts), flush=True)


def run_cmake(command: list[str]) -> str:
    try:
        result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except FileNotFoundError as error:
        raise RuntimeError("cmake was not found on PATH") from error
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"CMake command failed: {' '.join(command)}\n{error.stdout}") from error
    return result.stdout


def configured_target(build_directory: Path) -> str | None:
    cache = build_directory / "CMakeCache.txt"
    if not cache.is_file():
        return None
    match = FIND_TARGET_PATTERN.search(cache.read_text(errors="replace"))
    return match.group(1) if match is not None else None


def find_target_file(target: str, build_directory: Path) -> Path:
    build_directory = build_directory.resolve()
    if configured_target(build_directory) != target:
        run_cmake(["cmake", "-S", str(PROJECT_ROOT), "-B", str(build_directory), f"-DFIND_TARGET={target}", "-DCMAKE_BUILD_TYPE=Release"])
    output = run_cmake(["cmake", "--build", str(build_directory), "--target", "FindTarget"])
    match = TARGET_FILE_PATTERN.search(output)
    if match is None:
        raise RuntimeError(f"FindTarget did not return a file path for target '{target}':\n{output}")

    target_file = Path(match.group(1)).resolve()
    if not target_file.is_file():
        raise RuntimeError(f"FindTarget returned a target file that does not exist: {target_file}")
    return target_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configure CMake and print a target's built file path.")
    parser.add_argument("target", help="CMake target to locate")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIRECTORY, help="CMake build directory")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(find_target_file(args.target, args.build_dir))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"initialization failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error