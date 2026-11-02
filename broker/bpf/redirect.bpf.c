// Blind forwarder for the broker: every byte the broker receives from the
// master is handed to the attached slave, in the kernel, with no userspace
// copy and no parsing.
//
// Two maps, so the redirect is strictly one-directional:
//   upstreams[key 0] = the master -> broker socket. The sk_skb programs are
//                      attached to *this* map only, so a slave's own ingress is
//                      never verdict-processed and the redirect cannot loop.
//   slaves[0]        = the currently attached slave, the redirect target.
// Redirecting across maps is allowed; only the destination must live in the map
// named in the redirect helper.
//
// The destination has to be a SOCKMAP, not a SOCKHASH: the verifier's
// check_map_func_compatibility() allows only bpf_sk_redirect_hash to touch a
// SOCKHASH, and passing one to bpf_sk_redirect_map fails the load with -EINVAL.
// A single slot is an array index anyway, which is what a SOCKMAP is.

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

#define UPSTREAM_KEY 0
#define SLAVE_KEY 0

struct {
    __uint(type, BPF_MAP_TYPE_SOCKHASH);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u32);
} upstreams SEC(".maps");

// A SOCKMAP is an array of sockets: the key is an index below max_entries.
struct {
    __uint(type, BPF_MAP_TYPE_SOCKMAP);
    __uint(max_entries, 2);
    __type(key, __u32);
    __type(value, __u32);
} slaves SEC(".maps");

// Every skb is one complete message: pass it through whole.
SEC("sk_skb/stream_parser")
int broker_parser(struct __sk_buff *skb) {
    return skb->len;
}

// Redirect each received message to the attached slave. If no slave is attached
// (slot empty) the redirect fails and the message is dropped, which is exactly
// the /dev/null behaviour of a broker with no replica.
SEC("sk_skb/stream_verdict")
int broker_verdict(struct __sk_buff *skb) {
    return bpf_sk_redirect_map(skb, &slaves, SLAVE_KEY, 0);
}

char LICENSE[] SEC("license") = "Dual BSD/GPL";
