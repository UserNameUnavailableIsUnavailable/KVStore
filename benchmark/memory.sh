#!/usr/bin/env bash

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${SCRIPT_ROOT}

rm -rf build
cmake -S . -B build -G "Ninja Multi-Config"
cmake --build build --config Release
echo "---standard memory allocator---" >> ./benchmark/memory.txt
python3 benchmark/memory.py --count 1000000 --clients 1000 --sample-interval 0.01 --startup-timeout 10 --cycles 4 --port 6666 >> ./benchmark/memory.txt

rm -rf build
cmake -S . -B build -G "Ninja Multi-Config" -DCUSTOM_MEMORY_POOLING=ON
cmake --build build --config Release
echo "---customized memory allocator---" >> ./benchmark/memory.txt
python3 benchmark/memory.py --count 1000000 --clients 1000 --sample-interval 0.01 --startup-timeout 10 --cycles 4 --port 6666 >> ./benchmark/memory.txt

rm -rf build
cmake -S . -B build -G "Ninja Multi-Config" -DUSE_JEMALLOC=ON
cmake --build build --config Release
echo "---jemalloc memory allocator---" >> ./benchmark/benchmark.txt
python3 benchmark/memory.py --count 1000000 --clients 1000 --sample-interval 0.01 --startup-timeout 10 --cycles 4 --port 6666 >> ./benchmark/memory.txt

cd -