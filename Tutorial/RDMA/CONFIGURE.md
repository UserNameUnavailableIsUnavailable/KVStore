# RDMA prerequisites

```bash
sudo modprobe siw # iWARP
sudo rdma link add siw0 type siw netdev <dev>
ibv_devices
```

`<dev>` should be an available device.