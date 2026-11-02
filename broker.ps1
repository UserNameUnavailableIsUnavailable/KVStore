# The broker: registers with the master as a /dev/null replica and blindly
# forwards what it is pushed to whichever slave is attached.
#
#   sudo ./broker.ps1                 # in-kernel forward (sk_skb + SOCKHASH)
#   ./broker.ps1 -NoBpf               # userspace relay, no privileges needed
#
# sudo is what buys the in-kernel path: loading the BPF object needs CAP_BPF /
# CAP_SYS_ADMIN, and without it the broker logs "forwarding userspace relay" and
# keeps working with the same semantics at a higher per-byte cost.

param(
    [string]$Master = "192.168.4.146:6666",
    [string]$Listen = "192.168.4.146:7000",
    [switch]$NoBpf
)

$forward = @()
if ($NoBpf) {
    $forward += "--no-bpf"
}

sudo ./build/broker/Release/broker --master $Master --listen $Listen @forward @args
