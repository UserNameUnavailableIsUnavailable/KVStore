# 待优化项

- STL 默认的 hash table 出现哈希冲突、扩容时 RPS 抖动，需自定义 hash 策略。
- epoll 的事件数量、io_uring 的 SQ、CQ 大小动态调整。
- 协程库的跨平台支持。

I am thinking about a new master-slave replication pattern.
The slave connects to the master (TCP), then the master prepares a dump file and send to the slave via RDMA.
Meanwhile, an eBPF process keeps queuing packets sent to the master after the slave's connection request.
The slave loads the RDB dump and disconnects from the master.