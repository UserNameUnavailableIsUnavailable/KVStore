#!/bin/sh
SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
${SCRIPT_ROOT}/../build/Application/Server/Release/Server --config ./configs/master.conf --multiplexer epoll
