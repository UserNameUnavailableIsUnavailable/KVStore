// Extracts the master's realtime push stream at the point it leaves the stack:
// TC egress on loopback. The program watches TCP segments whose source port is
// the master's, copies their payload into a ring buffer and lets them pass
// untouched (TC_ACT_OK). Nothing is rewritten, dropped or redirected -- this is
// a copy-only tap, so the slave the master pushes to is unaffected.
//
// The daemon writes the master port into `config` before attaching; the program
// is inert until then. Every segment becomes one or more ring buffer events of
// `RELAY_CHUNK_SIZE` payload bytes, each carrying the TCP sequence and the
// slice's offset so a userspace reassembler can deduplicate and reorder.

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <linux/pkt_cls.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#include <relay/relay_event.h>

#define ETH_P_IP 0x0800

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 20);
} events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct relay_config);
} config SEC(".maps");

char LICENSE[] SEC("license") = "Dual BSD/GPL";

SEC("tc")
int relay_egress(struct __sk_buff *skb)
{
    void *data_end = (void *)(long)skb->data_end;
    void *data = (void *)(long)skb->data;

    struct ethhdr *eth = data; // the payload is IP datagram
    if ((void *)(eth + 1) > data_end) return TC_ACT_OK;
    // Currently support IPv4 only.
    if (eth->h_proto != bpf_htons(ETH_P_IP)) return TC_ACT_OK;

    // See also: https://datatracker.ietf.org/doc/html/rfc791
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return TC_ACT_OK;
    // intercepts TCP outbound traffic
    if (ip->protocol != IPPROTO_TCP) return TC_ACT_OK;

    if (ip->ihl < 5) return TC_ACT_OK; // minimum IPv4 header length is 5 (20 bytes)
    const __u32 ip_len = ip->ihl * 4; // internet header length

    struct tcphdr *tcp = (void *)((char *)ip + ip_len);
    if ((void *)(tcp + 1) > data_end) return TC_ACT_OK;

    const __u32 tcp_len = tcp->doff * 4;
    if (tcp_len < 20) return TC_ACT_OK;

    const __u32 zero = 0;
    struct relay_config *cfg = bpf_map_lookup_elem(&config, &zero);
    if (!cfg || cfg->master_port == 0) return TC_ACT_OK;
    if (tcp->source != bpf_htons((__u16)cfg->master_port)) return TC_ACT_OK;

    const __u32 data_off = sizeof(struct ethhdr) + ip_len + tcp_len;
    if (data_off > skb->len) return TC_ACT_OK;
    const __u32 payload_len = skb->len - data_off;
    if (payload_len == 0) return TC_ACT_OK;

    const __u32 conn_id = bpf_ntohs(tcp->dest);
    const __u32 seq = bpf_ntohl(tcp->seq);

    // Emit the payload in bounded slices. A segment is at most a few tens of
    // KiB, so eight 4 KiB slices is ample; if one overflows, the rest of the
    // segment is dropped and the gap shows up as an offset jump downstream.
    for (__u32 off = 0; off < RELAY_CHUNK_SIZE * 8; off += RELAY_CHUNK_SIZE) {
        if (off >= payload_len) break;

        __u32 chunk = payload_len - off;
        if (chunk > RELAY_CHUNK_SIZE) chunk = RELAY_CHUNK_SIZE;
        // Verifier-friendly: len is provably in [1, RELAY_CHUNK_SIZE].
        const __u32 len = ((chunk - 1) & (RELAY_CHUNK_SIZE - 1)) + 1;

        // Reserve the full record: the verifier knows e->data is exactly
        // RELAY_CHUNK_SIZE bytes, so bpf_skb_load_bytes(..., len <= CHUNK) is
        // provably in bounds. A variable-sized reservation cannot be proven the
        // same way and is rejected.
        struct relay_packet_event *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
        if (!e) return TC_ACT_OK;

        e->dest = conn_id;
        e->seq = seq;
        e->payload_len = payload_len;
        e->offset = off;
        e->chunk_len = len;
        bpf_skb_load_bytes(skb, data_off + off, e->data, len);
        bpf_ringbuf_submit(e, 0);
    }

    return TC_ACT_OK;
}
