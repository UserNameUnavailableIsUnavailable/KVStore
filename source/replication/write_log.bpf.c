#include <linux/bpf.h>
#include <asm/ptrace.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <replication/probe_event.h>

char LICENSE[] SEC("license") = "Dual BSD/GPL";

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024);
} records SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __u32 *key;
    struct kv_log_state* value;
} status SEC(".maps");

SEC("uprobe")
int write_log(struct pt_regs *ctx) {
    __u64 epoch = PT_REGS_PARM1(ctx);
    __u64 end = PT_REGS_PARM2(ctx);
    const char *data = (const char *)PT_REGS_PARM3(ctx);
    __u64 length = PT_REGS_PARM4(ctx);
    __u32 key = 0;
    struct kv_log_state *state = bpf_map_lookup_elem(&status, &key);
    if (!state) return 0;
    __u64 tid = bpf_get_current_pid_tgid(); // get current thread ID and process ID (tid: lower 32 bits, pid: upper 32 bits)
    if (state->owner_tid && state->owner_tid != tid) {
        __sync_fetch_and_add(&state->failures, 1);
        return 0;
    }
    state->owner_tid = tid;
    __sync_fetch_and_add(&state->revision, 1);
    if (!state->epoch) {
        state->epoch = epoch;
        state->head = end >= length ? end - length : 0;
    }
    if (state->epoch != epoch || end < length || end - length != state->head ||
        length > KV_LOG_MAX_PAYLOAD) {
        __sync_fetch_and_add(&state->failures, 1);
        goto done;
    }
    for (__u32 position = 0; position < KV_LOG_MAX_PAYLOAD; position += KV_LOG_CHUNK) {
        if (position >= length) break;
        struct kv_log_event *event = bpf_ringbuf_reserve(&records, sizeof(*event), 0);
        if (!event) {
            __sync_fetch_and_add(&state->failures, 1);
            break;
        }
        __u32 size = length - position;
        if (size > KV_LOG_CHUNK) size = KV_LOG_CHUNK;
        event->epoch = epoch;
        event->end_offset = end;
        event->length = length;
        event->position = position;
        event->size = size;
        if (bpf_probe_read_user(event->data, size, data + position)) {
            bpf_ringbuf_discard(event, 0);
            __sync_fetch_and_add(&state->failures, 1);
            break;
        }
        bpf_ringbuf_submit(event, 0);
    }
done:
    state->epoch = epoch;
    state->head = end;
    state->seen_ns = bpf_ktime_get_ns();
    __sync_fetch_and_add(&state->revision, 1);
    return 0;
}
