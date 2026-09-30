#!/usr/bin/env bash

set -euo pipefail

PORT="${PORT:-5201}"
JSON="${JSON:-0}"
ONE_OFF="${ONE_OFF:-0}"

command=(iperf3 -s -p "$PORT")

if [[ "$JSON" == "1" ]]; then
	command+=(--json)
fi

if [[ "$ONE_OFF" == "1" ]]; then
	command+=(--one-off)
fi

"${command[@]}"