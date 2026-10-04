#!/usr/bin/env bash

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${SCRIPT_ROOT}

set -euo pipefail

SERVER_IP="${SERVER_IP:-192.168.4.146}"
LOCAL_IP="${LOCAL_IP:-}"
PORT="${PORT:-7500}"
RDMA_DEVICE="${RDMA_DEVICE:-siw0}"
MESSAGE_BYTES="${MESSAGE_BYTES:-}"
MESSAGES="${MESSAGES:-}"
MULTIPLEXER="${MULTIPLEXER:-epoll}"

if [[ -z "$LOCAL_IP" ]]; then
	netdev="$(rdma link show | awk -v rdma_device="$RDMA_DEVICE" '$1 == "link" && $2 ~ ("^" rdma_device "/") { for (i = 1; i <= NF; ++i) if ($i == "netdev") { print $(i + 1); exit } }')"
	if [[ -n "$netdev" ]]; then
		LOCAL_IP="$(ip -4 -o addr show dev "$netdev" | awk 'NR == 1 { split($4, parts, "/"); print parts[1] }')"
	fi
fi

command=(
	./build/nbio/benchmark/Release/RdmaDeliverBenchmark
	--mode client
	--ip "$SERVER_IP"
	--port "$PORT"
	--rdma-device "$RDMA_DEVICE"
	--multiplexer "$MULTIPLEXER"
)

if [[ -n "$LOCAL_IP" ]]; then
	command+=(--local-ip "$LOCAL_IP")
fi

if [[ -n "$MESSAGE_BYTES" ]]; then
	command+=(--message-bytes "$MESSAGE_BYTES")
fi

if [[ -n "$MESSAGES" ]]; then
	command+=(--messages "$MESSAGES")
fi

"${command[@]}"

cd -