#!/bin/sh
SCRIPT_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_ROOT" || exit 1
exec ./build/source/server/Release/server --config ./configs/master.conf --multiplexer io_uring
