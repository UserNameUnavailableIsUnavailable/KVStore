# The relay daemon: taps the master's outbound push to a slave (TC egress on lo)
# and broadcasts the extracted bytes to slaves that connect to it. sudo buys the
# TC attach (CAP_NET_ADMIN); without it the daemon falls back to a plain TCP
# replica and says so at startup.
#
#   sudo ./relay.ps1                                     # tap master:6666 -> lo, listen 7100
#   sudo ./relay.ps1 -MasterPort 6666 -Listen 192.168.4.146:7100
#   ./relay.ps1 -NoSudo -Master 192.168.4.146:6666 -Listen 192.168.4.146:7100   # fallback, no privileges
#
# Topology: run the master, then ONE direct slave (the tap source), then the
# relay, then the remaining slaves with `./slave.ps1 -Broker <relay listen>`.

param(
    [string]$Listen = "192.168.4.146:7100",
    [string]$MasterPort = "6666",
    [string]$Ifname = "lo",
    [string]$Master,
    [switch]$NoSudo
)

$base = @("--listen", $Listen, "--master-port", $MasterPort, "--ifname", $Ifname)
if ($Master) {
    $base += "--master"
    $base += $Master
}

if ($NoSudo) {
    & ./build/source/relay/Release/relay @base @args
} else {
    sudo ./build/source/relay/Release/relay @base @args
}
