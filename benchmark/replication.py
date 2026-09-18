#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import redis
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from benchmark import DEFAULT_BUILD_DIRECTORY, find_target_file
from benchmark.save import BATCH_SIZE, DEFAULT_RESTART_WAIT, DEFAULT_SEED, random_value, require_redis


DEFAULT_MASTER_PORT = 6666
DEFAULT_REPLICA_PORT = 6668
DEFAULT_REPLICATION_PORT = 6667
DEFAULT_MASTER_CONFIG = PROJECT_ROOT / "benchmark" / "master.conf"
DEFAULT_REPLICA_CONFIG = PROJECT_ROOT / "benchmark" / "slave.conf"
DEFAULT_RECORDS_PER_PHASE = 50_000


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Verify KVStore snapshot and incremental RDMA replication.")
	parser.add_argument("--target", default="Server", help="CMake target for the KVStore server executable")
	parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIRECTORY, help="CMake build directory")
	parser.add_argument("--rdma-address", required=True, help="IPv4 address assigned to the RDMA device")
	parser.add_argument("--master-port", type=int, default=DEFAULT_MASTER_PORT, help="Master TCP port")
	parser.add_argument("--replica-port", type=int, default=DEFAULT_REPLICA_PORT, help="Replica TCP port")
	parser.add_argument("--replication-port", type=int, default=DEFAULT_REPLICATION_PORT, help="Master RDMA replication port")
	parser.add_argument("--master-config", type=Path, default=DEFAULT_MASTER_CONFIG, help="Master startup configuration")
	parser.add_argument("--replica-config", type=Path, default=DEFAULT_REPLICA_CONFIG, help="Replica startup configuration")
	parser.add_argument("--records-per-phase", type=int, default=DEFAULT_RECORDS_PER_PHASE,
						help="Records to write before and after the replica starts")
	parser.add_argument("--sync-timeout", type=float, default=DEFAULT_RESTART_WAIT,
						help="Seconds to wait for each replication phase")
	parser.add_argument("--keep-temp", action="store_true", help="Keep master/slave data and logs under PROJECT_ROOT/temp")
	return parser.parse_args()


def assert_port_available(port: int) -> None:
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
		try:
			listener.bind(("127.0.0.1", port))
		except OSError as error:
			raise RuntimeError(f"port {port} is already in use; stop the existing server before testing") from error


def assert_local_rdma_address(address: str) -> None:
	with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
		try:
			probe.bind((address, 0))
		except OSError as error:
			raise RuntimeError(f"RDMA address {address} is not assigned to a local interface") from error


def start_server(server: Path, directory: Path, command: list[str]) -> tuple[subprocess.Popen[str], Path, Path]:
	stdout_path = directory / "server.stdout.log"
	stderr_path = directory / "server.stderr.log"
	stdout = stdout_path.open("w")
	stderr = stderr_path.open("w")
	process = subprocess.Popen([str(server), *command], cwd=directory, stdout=stdout, stderr=stderr, text=True)
	stdout.close()
	stderr.close()
	return process, stdout_path, stderr_path


def stop_server(process: subprocess.Popen[str]) -> None:
	if process.poll() is not None:
		return
	process.terminate()
	try:
		process.wait(timeout=10)
	except subprocess.TimeoutExpired:
		process.kill()
		process.wait(timeout=10)


def wait_for_ping(client: object, process: subprocess.Popen[str], timeout: float) -> None:
	deadline = time.monotonic() + timeout
	while time.monotonic() < deadline:
		if process.poll() is not None:
			raise RuntimeError(f"server exited before accepting TCP requests (exit code {process.returncode})")
		try:
			if client.ping():
				return
		except Exception:
			time.sleep(0.1)
	raise RuntimeError(f"server did not accept TCP requests within {timeout:.1f} seconds")


def wait_for_value(client: object, key: str, expected: str, timeout: float, phase: str) -> None:
	deadline = time.monotonic() + timeout
	while time.monotonic() < deadline:
		if client.get(key) == expected:
			print(f"{phase} reached the replica", flush=True)
			return
		time.sleep(0.1)
	raise RuntimeError(f"replica did not receive {phase} within {timeout:.1f} seconds")


def write_records(client: object, rng: object, start: int, count: int, phase: str) -> tuple[str, str]:
	final_key = ""
	final_value = ""
	for batch_start in range(start, start + count, BATCH_SIZE):
		batch_end = min(batch_start + BATCH_SIZE, start + count)
		pipeline = client.pipeline(transaction=False)
		for index in range(batch_start, batch_end):
			key = f"record:{index:06d}"
			value = random_value(rng, index)
			pipeline.set(key, value)
			final_key, final_value = key, value
		if not all(pipeline.execute()):
			raise RuntimeError(f"a SET command failed during {phase}")
		print(f"{phase}: wrote {batch_end - start}/{count} records", flush=True)
	return final_key, final_value


def verify_records(client: object, count: int) -> None:
	rng = random.Random(DEFAULT_SEED)
	for batch_start in range(0, count, BATCH_SIZE):
		batch_end = min(batch_start + BATCH_SIZE, count)
		pipeline = client.pipeline(transaction=False)
		expected: list[tuple[str, str]] = []
		for index in range(batch_start, batch_end):
			key = f"record:{index:06d}"
			value = random_value(rng, index)
			pipeline.get(key)
			expected.append((key, value))
		actual = pipeline.execute()
		for (key, value), restored in zip(expected, actual):
			if restored != value:
				raise RuntimeError(f"replica value mismatch for {key}: expected {value!r}, got {restored!r}")
		print(f"validated {batch_end}/{count} replica records", flush=True)


def print_log(name: str, path: Path) -> None:
	print(f"--- {name} ---", file=sys.stderr)
	print(path.read_text(errors="replace") if path.exists() else "", file=sys.stderr)


def main() -> int:
	args = parse_args()
	if args.records_per_phase <= 0 or args.sync_timeout <= 0:
		raise RuntimeError("records per phase and sync timeout must be positive")
	for port in (args.master_port, args.replica_port, args.replication_port):
		if not 1 <= port <= 65535:
			raise RuntimeError("all ports must be between 1 and 65535")
		assert_port_available(port)
	assert_local_rdma_address(args.rdma_address)
	for config in (args.master_config, args.replica_config):
		if not config.is_file():
			raise RuntimeError(f"configuration was not found: {config}")

	require_redis()
	server = find_target_file(args.target, args.build_dir)
	temp_root = PROJECT_ROOT / "temp"
	temp_root.mkdir(exist_ok=True)
	root = Path(tempfile.mkdtemp(prefix="kvstore-replication-", dir=temp_root))
	master_directory = root / "master"
	replica_directory = root / "replica"
	master_directory.mkdir()
	replica_directory.mkdir()
	master_process: subprocess.Popen[str] | None = None
	replica_process: subprocess.Popen[str] | None = None
	master_logs: tuple[Path, Path] | None = None
	replica_logs: tuple[Path, Path] | None = None

	try:
		master_process, *master_logs = start_server(
			server, master_directory,
			["--port", str(args.master_port), "--replication-port", str(args.replication_port),
			 "--replication-address", args.rdma_address, "--config", str(args.master_config.resolve())],
		)
		master = redis.Redis(host="127.0.0.1", port=args.master_port, decode_responses=True, protocol=2)
		wait_for_ping(master, master_process, args.sync_timeout)

		rng = random.Random(DEFAULT_SEED)
		first_key, first_value = write_records(master, rng, 0, args.records_per_phase, "initial phase")

		replica_process, *replica_logs = start_server(
			server, replica_directory,
			["--port", str(args.replica_port), "--replicaof", f"{args.rdma_address}:{args.replication_port}",
			 "--config", str(args.replica_config.resolve())],
		)
		replica = redis.Redis(host="127.0.0.1", port=args.replica_port, decode_responses=True, protocol=2)
		wait_for_ping(replica, replica_process, args.sync_timeout)
		wait_for_value(replica, first_key, first_value, args.sync_timeout, "initial snapshot")

		last_key, last_value = write_records(master, rng, args.records_per_phase, args.records_per_phase, "incremental phase")
		wait_for_value(replica, last_key, last_value, args.sync_timeout, "incremental replication")
		verify_records(replica, 2 * args.records_per_phase)
		print(f"replication restored all {2 * args.records_per_phase} records", flush=True)
		return 0
	except Exception:
		if master_logs is not None:
			print_log("master stdout", master_logs[0])
			print_log("master stderr", master_logs[1])
		if replica_logs is not None:
			print_log("replica stdout", replica_logs[0])
			print_log("replica stderr", replica_logs[1])
		raise
	finally:
		if replica_process is not None:
			stop_server(replica_process)
		if master_process is not None:
			stop_server(master_process)
		if args.keep_temp:
			print(f"temporary directory: {root}", flush=True)
		else:
			shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
	try:
		raise SystemExit(main())
	except RuntimeError as error:
		print(f"replication test failed: {error}", file=sys.stderr)
		raise SystemExit(1) from error
