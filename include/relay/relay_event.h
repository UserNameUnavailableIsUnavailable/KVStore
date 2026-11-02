#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// The largest slice one ring buffer event carries. The BPF program emits one
// event per TCP segment, chunking larger segments into events of at most this
// many payload bytes. The record is a fixed RELAY_CHUNK_SIZE bytes so the
// verifier can prove the memcpy bound; a small command still pays for the full
// record, which is a TODO to shrink once the variable-size pattern is verified.
#define RELAY_CHUNK_SIZE 4096

// One extracted slice of a mirrored TCP payload.
//
// `conn_id` is the master's destination port, which is what distinguishes the
// master's push to one slave from its push to another (the source is always the
// master's own port). `seq` is the TCP sequence number of the segment the slice
// came from, `payload_len` that segment's total payload and `offset` this
// slice's position inside it -- the fields a userspace reassembler needs to
// deduplicate retransmissions, reorder out-of-order slices, and detect holes.
//
// On loopback, where the master talks to its slaves in this setup, TCP is
// ordered and lossless, so appending data[0..chunk_len) in arrival order
// reproduces the master's outbound stream exactly; the seq fields are carried
// for the day the tap runs on a real interface where that no longer holds.
struct relay_packet_event {
    uint32_t dest; // TCP destination port
    uint32_t seq; // TCP sequence number
    uint32_t payload_len;
    uint32_t offset;
    uint32_t chunk_len;
    uint8_t data[RELAY_CHUNK_SIZE];
};

// The BPF program's single configuration knob, written by the daemon at attach
// time. `master_port` is in host byte order.
struct relay_config {
    uint32_t master_port;
};

#ifdef __cplusplus
}
#endif
