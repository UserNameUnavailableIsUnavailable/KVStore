#!/bin/sh
SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
./build/Application/server/Release/server --config ./configs/master.conf --multiplexer io_uring
