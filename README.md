# My KVStore Implementation

## See Also

- [RESP (Redis Serialization Protocol)](https://redis.io/docs/latest/develop/reference/protocol-spec/)

## Build

This project depends on vcpkg, see [vcpkg](https://vcpkg.io/en/) for a installation guide.

With `VCPKG_ROOT` set, run the following command to configure and build:

```bash
cmake -S . -B build
cmake --build build
```

## Testing

This project has GoogleTest integration, run the following command to test:

```bash
ctest --test-dir build/Common
```