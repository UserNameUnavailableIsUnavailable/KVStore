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

历史架构提案见 [eBPF 辅助的有序变更复制设计](docs/replication-design.md)。
不代表当前实现已具备其中的保证。

实验性 C++ `repld` 通过 uprobe 观察显式逻辑日志函数，实现 TCP 增量复制；RDMA 仍负责全量同步。
通过 `--replication-daemon IPv4:port` 启用，省略该参数保留旧 RDMA 增量路径。
AOF 关闭且共享副本计数为零时跳过日志编码；独立 daemon 捕获并转发。构建需要支持 BPF 的 clang 和 libbpf，
运行需要 BPF 权限。目前受限环境尚未完成内核加载验证。
使用方法与限制见 [逻辑日志 uprobe 复制指南](docs/replication-uprobe.md)。

## 构建

### 第三方库

项目使用 vcpkg 进行包管理。若没有安装 vcpkg，可通过如下命令快速安装：

```bash
git clone https://github.com/microsoft/vcpkg.git /path/to/vcpkg --depth 1
cd /path/to/vcpkg
./bootstrap-vcpkg.sh
export VCPKG_ROOT=/path/to/vcpkg # 设置 vcpkg 根目录环境变量
```

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

<!-- 以下为通过系统 `pkg-config` 解析的 Linux 依赖（构建前需安装）：

| 系统包 | 用途 |
| --- | --- |
| `librdmacm` + `libibverbs`（`librdmacm-dev`、`libibverbs-dev`） | RDMA 复制与 wire 测试 |
-->

### RDMA 硬件

复制与 `RDMA*` 测试需要 RDMA 设备，可通过软件模拟：

```bash
sudo rdma link add siw0 type siw netdev {net_device}
```

### 构建流程

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

项目目录：

- `include/`：应用依赖模块的头文件。
- `source/`：依赖模块实现、模块构建配置和测试。
- `applications/`：各应用的入口和 facade。`server.cpp` 负责配置并启动
  `source/server/` 中的服务实现；`replication_daemon.cpp` 调用 `source/replication/`
  中由 `replication_daemon_impl` INTERFACE 库提供的实现。

多配置构建的可执行文件位于 `build/applications/<config>/server`；
单配置构建位于 `build/applications/server`。启动脚本使用 Release 多配置路径。

Linux 下的 `repld` 目标使用 libbpf 和 NBIO。
仅构建 RDMA 路径时可设置 `-DKVSTORE_BUILD_PROBE_DAEMON=OFF`，不需要 BPF 依赖。

```bash
cmake --build build --config Release --target repld
./build/applications/Release/repld --help
```

master 启动时自动运行同目录的 `repld`（可用 `--repld PATH` 覆盖），
传递共享内存描述符并挂载 `kvstore_write_log_v1`；不解析 C++ Command 对象内存布局。
双方通过 mmap 共享 `ReplicationInfo`：master 在快照同步前增加副本计数，
交接失败时由 master 减少；`repld` 只继承计数，不增加，副本断开时由它减少。
daemon 意外退出会使 master 显式停止，避免继续提供未经捕获的写入。
网络服务使用 NBIO 协程，继承 master 的 `--multiplexer epoll|io_uring` 后端。
捕获丢失或 backlog 不足会显式触发重新全量同步。
配置与限制见 [逻辑日志 uprobe 复制指南](docs/replication-uprobe.md)。

```bash
# 启动 master，使用 epoll 多路复用器
./build/applications/Release/server --config ./configs/master.conf --multiplexer epoll
# 启动 slave，使用 io_uring 多路复用器
./build/applications/Release/server --config ./configs/slave.conf --multiplexer io_uring
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
