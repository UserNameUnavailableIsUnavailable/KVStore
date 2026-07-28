# KVStore Design

## Common

This KVStore design has the following layers:

- Session Layer: The client initiates a session with the server, during which the client sends requests, and the server responds.
  - Connection Control, Address, Port, Timeout, etc.
  - Networking Model: For now, we only support the Linux platform.
    - Reactor: `epoll` + C++ 20 coroutine-based event loop.
    - Proactor: `io_uring`.
  - Protocol: Networking model agnostic, built on top of the networking model, support the simplified RESP protocol.
    - Request: The client sends commands to the server. Commands are encapsulated in request objects, serialized by the sender, and deserialized by the receiver.
    - Response: Serialization / deserialization of results. Results are encapsulated in response objects. Serialization / deserialization of results.
- Execution Layer: The following operations are supported:
    - CONFIG <key> <value>: Configures the KVStore.
      - AOF <file>: Enable AOF.
      - AOF ON|OFF: Enable AOF.
    - EXISTS <key>: Returns whether the key exists.
    - GET <key>: Returns the value of the key.
    - SET <key> <value>: Returns nothing.
    - DELETE <key>: Returns the value of the key.
    - EXPIRE <key> <ttl>: Returns the TTL of the key.
    - TTL <key>: Returns the TTL of the key.
    - SAVE: Trigger a snapshot of the KVStore.
- Cache Layer: Responsible for executing commands, lifecycle management.
  - Operations: The following operations are supported, each operation starts with a status and a simple message indicating the success or failure of the operation.
  - Data structure: The following four data structures are supported:
    - Skip List: Key-value pairs are stored in a skip list, suitable for range queries.
    - Red-Black Tree: Key-value pairs are stored in a red-black tree, balanced choice.
    - Array: Key-value pairs are stored in an array, suitable for small scale data.
    - Hash: Key-value pairs are stored in a hash table, suitable for large scale data.
  - Lifecycle Management:
    - TTL: Each key-value has a TTL, which is used to determine when the key-value pair should be expired.
    - Eviction strategies: Commands may trigger expiration strategies.
      - LRU
      - LFU
- Persistent Layer:
  - Full Dump: The entire KVStore is dumped to a file.
  - Incremental Dump: The KVStore is dumped to a file incrementally, i.e., the execution record of each command.
- Primary-Replica Layer: The primary and replica are used to ensure data consistency.
- Memory Pooling Layer: The memory pool is used to allocate memory for the KVStore. We utilize the C++ pmr feature.

We all put most of the above layers in `Common` so that they can be shared between the client and the server.

## Server

The server combines the above layers into a single application.

Use CLI11 to support the following command-line arguments:

- `--port <port>`: The port to listen on. Default is `6666`.
- `networking-model reactor|proactor`: The networking model to use.
- `--cache-strategy skip-list|red-black-tree|array|hash`: The cache layer to use.
- `--persistent-strategy full-dump|incremental-dump`: The persistent layer to use.
- `--replica-of <ip>:<port>`: The replica of the primary.
- `--memory-pooling-strategy pmr`: The memory pooling layer to use.