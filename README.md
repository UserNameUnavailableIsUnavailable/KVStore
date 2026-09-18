# KVStore

[English](README_en.md) | **中文**

一个用 C++20 写的 RESP 兼容键值服务器：客户端走 TCP + RESP，节点之间走 RDMA，
数据落在快照（RDB）与追加日志（AOF）里，写入会实时同步到副本。

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

复制与 `RDMA_*` 测试需要一块 RDMA 设备或软件模拟：

```bash
sudo sudo rdma link add siw0 type siw netdev <dev>
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
| `Echo`、`Sleep`、`Grace`、`SignalSvc`、`ScopedSignalService`、`Condition` | NBIO 示例 |
| `RDMA_Echo`、`RDMA_AsyncEcho` | RDMA 示例 |
| `Tutorial_RDMA_Server`、`Tutorial_RDMA_Client` | `Tutorial/RDMA` 的原生 verbs 例子 |

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
    --port 8080 --replication-port 8081 --replication-address 192.168.0.201
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

## 文档

- `Documentation/REPLICATION.md` — RDMA 全量与增量同步
- `Documentation/CONFIG.md` — 启动命令文件与两条启动期设置
- `Documentation/RDMA.md`、`Foundation/Core/RDMA.md` — RDMA 后端与运行时
- `Documentation/PSYNC.md`、`Documentation/SLAVEOF.md` — 早期 TCP 复制设计（未实现）
- `DESIGN.md`、`PITFALLS.md` — 设计取舍与踩过的坑
