# RDMA

- data path
- control path

- device: RDMA-capable Network Interface Card (RNIC)
- context: logical handle containing resources of the device.
- protection domain: restricts memory regions accessible by queue pairs.
- verbs: how to interact with RDMA hardware.
  - data path: send; recv; poll completion queue.
  - control path: open device; create queue pair; register memory region.
- communication id: (device, event channel, queue pair)
- event channel
- completion queue
- queue pair
- completion channel