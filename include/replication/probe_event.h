#pragma once

#include <linux/types.h>

#define KV_LOG_CHUNK 4096
#define KV_LOG_MAX_PAYLOAD (16U * 1024U * 1024U)

struct kv_log_event {
    __u64 epoch;
    __u64 end_offset;
    __u64 length;
    __u32 position;
    __u32 size;
    char data[KV_LOG_CHUNK];
};

struct kv_log_state {
    __u64 revision;
    __u64 epoch;
    __u64 head;
    __u64 failures;
    __u64 seen_ns;
    __u64 owner_tid;
};
