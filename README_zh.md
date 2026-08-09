# KVStore

## 项目概览

KVStore 是一个使用 C++ 实现的轻量级 RESP 兼容键值服务器。它提供简洁的命令接口、多个存储后端、可选持久化以及基础复制能力，适合学习和基础场景使用。

## 主要特性

- 支持 RESP 风格命令协议
- 支持多种索引实现：数组、哈希表、红黑树、跳表
- 支持两种网络后端：epoll 和 io_uring
- 支持通过快照和 AOF 做持久化
- 支持通过 SLAVEOF 和 PSYNC 做基础复制

## 构建

本项目依赖 vcpkg。请先确保 VCPKG_ROOT 已设置，然后执行：

```bash
cmake -S . -B build
cmake --build build --target KVServer
```

## 启动服务器

```bash
./build/Server/KVServer --port 8080 --networking-model epoll --cache-strategy hash
```

常用参数包括：

- `--persistent-dir <dir>`：指定持久化目录
- `--allocator default|pool|jemalloc`：选择分配器
- `--memory-pooling` 和 `--memory-pool-size <bytes>`：启用预分配内存池

## 启动客户端

```bash
./build/Client/KVClient 127.0.0.1 8080
```

随后可以输入以下命令：

```text
SET foo bar
GET foo
DEL foo
EXPIRE foo 10
QUIT
```

## 基础使用说明

- `SAVE` 会将当前数据写入完整转储文件。
- `APPENDONLY YES` 会开启命令追加日志，便于恢复。
- `SLAVEOF <host> <port>` 可从另一台服务器进行复制。

## 测试

```bash
ctest --test-dir build/Common
```