#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUILD_DIRECTORY = PROJECT_ROOT / "build"
TARGET_FILE_PATTERN = re.compile(r"^TARGET_FILE=(.+)$", re.MULTILINE)
FIND_TARGET_PATTERN = re.compile(r"^FIND_TARGET:[^=]*=(.*)$", re.MULTILINE)


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