# 待优化项

- STL 默认的 hash table 出现哈希冲突、扩容时 RPS 抖动，需自定义 hash 策略。
- epoll 的事件数量、io_uring 的 SQ、CQ 大小动态调整。
- 协程库的跨平台支持。