#!/usr/bin/env bash

set -euo pipefail

SERVER_IP="${SERVER_IP:-192.168.4.146}"
PORT="${PORT:-5201}"
DURATION="${DURATION:-20}"
PARALLEL="${PARALLEL:-1}"
REVERSE="${REVERSE:-0}"
ZEROCOPY="${ZEROCOPY:-0}"
JSON="${JSON:-0}"

command=(
	iperf3
	-c "$SERVER_IP"
	-p "$PORT"
	-t "$DURATION"
	-P "$PARALLEL"
)

if [[ "$REVERSE" == "1" ]]; then
	command+=(-R)
fi

if [[ "$ZEROCOPY" == "1" ]]; then
	command+=(--zerocopy)
fi

if [[ "$JSON" == "1" ]]; then
	command+=(--json)
fi

"${command[@]}"
