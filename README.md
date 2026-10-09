# KVStore

[English](README_en.md) | **中文**

基于 [NBIO](https://github.com/UserNameUnavailableIsUnavailable/NBIO) 构建的 KV 缓存引擎。兼容 Redis 的常用命令。

## 特性

- yieldable RESP 协议解析器：数据不完整时挂起（co_yield），补齐后继续解析，有效处理 TCP 粘包/拆包。

- 索引和淘汰策略：跳表、红黑树、哈希表可选作存储索引；支持 LRU / LFU 淘汰策略。

- 内存池：jemalloc 或定制的内存分配器，缓存频繁分配与释放的内存块，避免频繁内存分配。

- Slab 对象池：池化高频对象，避免频繁分配与释放的开销。

- 全量备份（即 Redis RDB）：在辅助线程中 fork，子进程完成序列化写盘，主线程协程通过条件等待/唤醒原语获知结果；启动时用 mmap 加载 RDB 并解析。加载备份文件时采用 mmap 策略。

- 增量备份（即 Redis AOF）：每条写命令编码为一条完整日志条目，命令协程等待该条 pwrite 完成后再应答，保证条目完整、有序、可回放；通道排队策略下吞吐良好。

- 主从复制：master 与 replica 通过 RDMA 通道通信，先经 RDMA 全量同步 RDB 文件，随后基于 backlog 策略进行实时增量同步。

## 构建依赖

### 系统与工具链

- **操作系统**：Linux（内核版本 >= 5.2，建议使用 6.0 及以上的版本以获得完整的 io_uring 特性支持）。服务器、复制与 io_uring 后端都在 `#if defined(__linux__)`
  保护之下，其它平台上只有 NBIO 的一部分能编译。
- **CMake ≥ 3.20**（顶层 `CMakeLists.txt` 声明；开发机实测 4.2.3）。
- **支持 C++20 的编译器**：代码用到协程、ranges、指定初始化与 concepts（实测 Clang 21.1.8；
  GCC 11+ / Clang 14+ 具备所需特性）。
- **Ninja**（可选，任意 CMake 生成器均可；实测 1.13.2）。

### Linux 开发包

RDMA、io_uring 与 BPF 相关库通过系统 `pkg-config` 解析（不走 vcpkg），
请先安装开发包：

```bash
sudo apt-get update
sudo apt-get install -y rdma-core libibverbs-dev librdmacm-dev liburing-dev libbpf-dev pkg-config
```

### vcpkg 与第三方库

项目使用 vcpkg 进行包管理。若没有安装 vcpkg，可通过如下命令快速安装：

```bash
git clone https://github.com/microsoft/vcpkg.git /path/to/vcpkg --depth 1
cd /path/to/vcpkg
./bootstrap-vcpkg.sh
export VCPKG_ROOT=/path/to/vcpkg # 设置 vcpkg 根目录环境变量
```

<!-- 以下为通过系统 `pkg-config` 解析的 Linux 依赖（构建前需安装）：

| 系统包 | 用途 |
| --- | --- |
| `librdmacm` + `libibverbs`（`librdmacm-dev`、`libibverbs-dev`） | RDMA 复制与 wire 测试 |
| `liburing`（`liburing-dev`） | io_uring 多路复用器（`URingMultiplexer`） |
| `libbpf`（`libbpf-dev`） | BPF 相关实验 | -->

### RDMA 硬件

复制与 `RDMA*` 测试需要 RDMA 设备，可通过软件模拟：

```bash
sudo rdma link add siw0 type siw netdev {net_device}
```

## 构建

可在 `vcpkg.json` 中调整 NBIO 的 io_uring 和 RDMA 特性支持：

```json
    {
      "name": "nbio",
      "features": [
        "io-uring",
        "rdma"
      ],
      "platform": "linux"
    }
```

CMake 支持的 flags：

- `-DUSE_JEMALLOC`：使用 jemalloc 作为内存分配器。
- `-DCUSTOM_MEMORY_POOLING`：使用自定义内存池作为内存分配器。
- `-DUSE_ARRAY_MAP_INDEX`：使用数组映射作为存储索引。
- `-DUSE_RED_BLACK_TREE_INDEX`：使用红黑树作为存储索引。
- `-DUSE_SKIP_LIST_INDEX`：使用跳表作为存储索引。
- `-DUSE_HASH_MAP_INDEX`：使用哈希表作为存储索引。

```bash
export VCPKG_ROOT=/path/to/vcpkg
cmake -S . -B build -G "Ninja Multi-Config"
```

```bash
cmake --build build --config Release
```

## 测试

```bash
ctest --test-dir build --output-on-failure

export KVSTORE_RDMA_ADDRESS=192.168.0.201
ctest --test-dir build --output-on-failure
```

`KVSTORE_LOG_LEVEL=debug|info|warn` 控制日志级别；传输层在 `debug` 下会打印每个包与回执。

## 运行

```bash
# 启动 master，使用 epoll 多路复用器
./build/Application/server/Release/server --config ./configs/master.conf --multiplexer epoll
# 启动 slave，使用 io_uring 多路复用器
./build/Application/server/Release/server --config ./configs/slave.conf --multiplexer io_uring
```

配置文件在 `configs/` 下，参考样例：

```conf
CONFIG SET PORT 6666 # Master 服务端口
CONFIG SET RDMA_DEVICE siw0 # RDMA 设备
CONFIG SET REPLICATION_ADDRESS 192.168.4.146 6667 # replica 服务 IP 地址和端口
CONFIG SET APPENDONLY NO # 是否开启 AOF
CONFIG SET AOF_CHECKSUM NO # 是否开启 AOF 校验
CONFIG SAVE 100 1000 # 每隔 100 秒保存至少 1000 次修改，与 Redis 行为类似
```

## 基准测试

`benchmark/` 下包含基准测试脚本和结果。

| 基准 | 脚本 | 结果 |
| --- | --- | --- |
| 吞吐：`redis-benchmark`，含多路复用器对比、流水线深度（对照 Redis）、AOF、周期备份 | `benchmark/pipeline.sh` 等 | [`benchmark/RSP.md`](benchmark/RSP.md) |
| 内存：多轮插入/删除下的 RSS 与复用情况，对比无池化、自定义池化与 jemalloc | `benchmark/memory.py` | [`benchmark/memory.md`](benchmark/memory.md) |
