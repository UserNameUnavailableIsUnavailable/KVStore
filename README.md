# KVStore

[English](README_en.md) | **中文**

## C++20 协程异步框架（NBIO）

I/O 多路复用同时支持 epoll（reactor）与 io_uring（proactor）两种后端，启动时二选一。多路复用器通过单工通道（TcpSendChannel / TcpReceiveChannel 等）完成事件派发：I/O 请求在通道内排队，复用器收到内核事件后回调将挂起的协程置为就绪。

reactor 路径（epoll）将通道内排队的 I/O 汇聚为一次 readv / writev / pwritev 提交；proactor 路径（io_uring）则在准备阶段直接批量下发全部排队请求，并按完成事件推进读写偏移。

C++20 协程任务调度器：支持对称转移（final_suspend 返回 continuation，以尾调用方式衔接）与任务取消；并提供 WhenAll / WhenAny 结构化并发组合子，作用域退出时自动取消剩余子任务。

定时任务通道：timerfd + 优先队列，配合任务取消机制实现超时取消。

信号通道：signalfd 信号通道、eventfd 通知通道，结合定时任务实现优雅退出。

条件变量：基于 eventfd 的 ConditionVariable 提供跨线程协程条件等待/唤醒原语。

网络、文件 I/O 通道：异步的 TcpSocket 收发、文件读写。

RDMA 通道：基于 iWARP 协议的 RDMA 收发通道。每个监听/连接端点持有独立的发送、接收内存池（由该端点接纳的连接共享这一对池），内存池基于 bitmap 管理块的分配与回收。

协程帧池化策略：采用类似于指数平均的池化策略，回收时缓存协程帧供后续使用，根据协程帧使用情况动态回收。

## 上层应用：KV 存储引擎

yieldable RESP 协议解析器：数据不完整时挂起（co_yield），补齐后继续解析，有效处理 TCP 粘包/拆包。
索引和淘汰策略：跳表、红黑树、哈希表可选作存储索引；支持 LRU / LFU 淘汰策略。

Slab 对象池：池化高频对象，避免频繁分配与释放的开销。

全量备份（即 Redis RDB）：在辅助线程中 fork，子进程完成序列化写盘，主线程协程通过条件等待/唤醒原语获知结果；启动时用 mmap 加载 RDB 并解析。加载备份文件时采用 mmap 策略。

增量备份（即 Redis AOF）：每条写命令编码为一条完整日志条目，命令协程等待该条 pwrite 完成后再应答，保证条目完整、有序、可回放；通道排队策略下吞吐良好。

主从复制：master 与 replica 通过 RDMA 通道通信，先经 RDMA 全量同步 RDB 文件，随后基于 backlog 策略进行实时增量同步。

## 构建依赖

### 系统与工具链

- **操作系统**：Linux（x86-64）。服务器、复制与 io_uring 后端都在 `#if defined(__linux__)`
  保护之下，其它平台上只有 Foundation 的一部分能编译。
- **CMake ≥ 3.20**（顶层 `CMakeLists.txt` 声明；开发机实测 4.2.3）。
- **支持 C++20 的编译器**：代码用到协程、ranges、指定初始化与 concepts（实测 Clang 21.1.8；
  GCC 11+ / Clang 14+ 具备所需特性）。
- **Ninja**（可选，任意 CMake 生成器均可；实测 1.13.2）。

### vcpkg 与第三方库

项目使用 vcpkg 的**清单模式**：依赖写在 `vcpkg.json`，安装目录固定在仓库内的
`vcpkg_installed/`，三元组为 `x64-linux`。构建前只需让 `VCPKG_ROOT` 指向本机的 vcpkg——
顶层 CMakeLists 会用它设置 `CMAKE_TOOLCHAIN_FILE`，之后的安装与编译由 CMake 自动完成。

| 依赖 | 用途 |
| --- | --- |
| `cli11` | 命令行解析（`Server`、`Client`） |
| `spdlog` | 日志 |
| `gtest` | 单元测试（`BUILD_TESTING`） |
| `benchmark` | 基准测试（Google Benchmark） |
| `crcpp` | 快照文件 CRC-64 校验 |
| `nlohmann-json` | JSON |
| `jemalloc` | 可选的进程级分配器（`LD_PRELOAD`） |
| `rdma-core`（Linux） | `libibverbs` / `librdmacm`：RDMA 复制与 wire 测试 |
| `liburing`（Linux） | io_uring 多路复用器（`URingMultiplexer`） |
| `libbpf`（Linux） | BPF 相关实验 |

### RDMA 硬件

复制与 `RDMA*` 测试需要一块 RDMA 设备或软件模拟：

```bash
sudo rdma link add siw0 type siw netdev <dev>
```

## 构建

```bash
export VCPKG_ROOT=/path/to/vcpkg
cmake -S . -B build -G Ninja
cmake --build build
```

常用目标：

| 目标 | 说明 |
| --- | --- |
| `Server` | 服务器：`build/Application/Server/Server` |
| `Client` | 命令行客户端：`build/Application/Client/Client` |
| `FoundationTesting`、`ServerTesting` | 单元测试可执行文件 |
| `Echo`、`Sleep`、`Grace`、`SystemSignalSvc`、`ScopedSystemSignalService`、`Condition` | NBIO 示例 |
| `RdmaEcho`、`RdmaAsyncEcho` | RDMA 示例 |
| `Tutorial_RdmaServer`、`Tutorial_RdmaClient` | `Tutorial/RDMA` 的原生 verbs 例子 |
| `RdmaFileBenchmark` | RDMA 链路吞吐基准（NBIO 层），需 `-DKVSTORE_BUILD_BENCHMARKS=ON` |

## 测试

```bash
ctest --test-dir build --output-on-failure     # 全部用例；RDMA 的 4 个会跳过

export KVSTORE_RDMA_ADDRESS=192.168.0.201      # 有 RDMA 设备时，让它们真正跑起来
ctest --test-dir build --output-on-failure
```

`KVSTORE_LOG_LEVEL=debug|info|warn` 控制日志级别；传输层在 `debug` 下会打印每个包与回执。

## 运行

```bash
# 单机
./build/Application/Server/Server --port 8080

# 主从（同一台机器上的两个实例）
./build/Application/Server/Server \
    --port 8080 --replication-port 8081 --replication-ip 192.168.0.201
./build/Application/Server/Server \
    --port 8082 --replicaof 192.168.0.201:8081

# 客户端：Client [host] [port]，默认 127.0.0.1:6379
./build/Application/Client/Client 127.0.0.1 8080
```

服务器也可以完全由启动命令文件驱动——每一行都是一条命令，管道里的命令同样能在文件里写：

```bash
./build/Application/Server/Server -c Application/master.conf
```

`Application/master.conf`（对外提供服务，并在 RDMA 设备上服务副本）：

```
config port 8080
config replication_address 192.168.0.201 8081
config appendonly yes
```

`Application/replica.conf`（只读副本，跟随上面的主节点）：

```
config port 8082
replicaof 192.168.0.201 8081
```

## 基准测试

C++ 基准默认不参与构建，配置时打开开关即可：

```bash
export KVSTORE_RDMA_ADDRESS=192.168.0.201
export KVSTORE_RDMA_DEVICE=siw2
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DKVSTORE_BUILD_BENCHMARKS=ON
cmake --build build --target RdmaFileBenchmark
./build/Foundation/NBIO/benchmark/Release/RdmaFileBenchmark
```

`RdmaFileBenchmark` 量的是 RDMA 链路本身，走的就是复制链路所用的那套 NBIO channel，两端
都在同一个进程里、各占一个引擎一个线程：一条连接，一个 connector 一个 acceptor，默认
1 GiB 随机载荷，两端各自报吞吐。`--size` 是载荷字节数，`--file` 改为从磁盘读，
`--multiplexer` 选 epoll 或 io_uring，`--port` 指定端口（0 表示自动挑一个空闲的）。
消息大小不是选项：它就是 resource manager 的 chunk 大小——发送拿到的是它，接收落进去的
也是它。

发送端会把自己的“未被确认”消息数压在接收端已投递的 receive 之内，和 `RdmaTransfer` 用的
是同一个窗口——因为一条消息到达时若接收端没有已投递的 receive，它既不会被排队也不会被
拒绝，而是直接把 queue pair 以 `RNR_RETRY_EXC_ERR` 拆掉。跑挂的运行会带着两端各自走到哪
一步退出，而不是一直挂着。`benchmark/` 下的 Python 脚本测的是整个服务器，结果记在
`benchmark/result.md`。

## 文档

- `Documentation/REPLICATION.md` — RDMA 全量与增量同步
- `Documentation/CONFIG.md` — 启动命令文件与两条启动期设置
- `Documentation/RDMA.md`、`Foundation/Core/RDMA.md` — RDMA 后端与运行时
- `Documentation/PSYNC.md`、`Documentation/SLAVEOF.md` — 早期 TCP 复制设计（未实现）
- `DESIGN.md`、`PITFALLS.md` — 设计取舍与踩过的坑
