#!/bin/sh
SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
./build/Application/server/Release/Server --config ./configs/master.conf --multiplexer epoll
