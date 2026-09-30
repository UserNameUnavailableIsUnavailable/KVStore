#!/usr/bin/env bash

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${SCRIPT_ROOT}

set -euo pipefail

SERVER_IP="${SERVER_IP:-192.168.4.146}"
PORT="${PORT:-7500}"
RDMA_DEVICE="${RDMA_DEVICE:-siw0}"
MESSAGE_BYTES="${MESSAGE_BYTES:-}"
MESSAGES="${MESSAGES:-}"
MULTIPLEXER="${MULTIPLEXER:-epoll}"

command=(
	./build/Foundation/NBIO/benchmark/Release/RdmaDeliverBenchmark
	--mode server
	--ip "$SERVER_IP"
	--port "$PORT"
	--rdma-device "$RDMA_DEVICE"
	--multiplexer "$MULTIPLEXER"
)

if [[ -n "$MESSAGE_BYTES" ]]; then
	command+=(--message-bytes "$MESSAGE_BYTES")
fi

if [[ -n "$MESSAGES" ]]; then
	command+=(--messages "$MESSAGES")
fi

"${command[@]}"

cd -