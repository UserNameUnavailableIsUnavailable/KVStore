# The master: serves snapshots over RDMA and pushes every write it executes to
# each directly attached slave. Needs no privileges.
#
#   ./master.ps1

./build/applications/Release/server --config ./configs/master.conf --multiplexer io_uring @args
