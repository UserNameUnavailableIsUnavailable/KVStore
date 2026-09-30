#!/usr/bin/env bash

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${SCRIPT_ROOT}

CMD="redis-benchmark -t set,get -n $N -P $P -q"
N=10000000
for P in 10 20 40 80 160; do
echo "KVStore"
$CMD -h 127.0.0.1 -p 6666
echo "Redis"
$CMD
done

cd -
