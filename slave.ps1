# A replica. By default it streams straight from the master; -Broker <ip:port>
# routes the stream through the broker instead.
#
#   ./slave.ps1                        # direct master -> slave
#   ./slave.ps1 -Broker 192.168.4.146:7000  # through the broker

param(
    [string]$Broker
)

$stream = @()
if ($Broker) {
    $stream += "--broker"
    $stream += $Broker
}

./build/applications/Release/server --config ./configs/slave.conf --multiplexer io_uring @stream @args
