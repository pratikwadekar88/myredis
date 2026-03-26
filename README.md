# MyRedis (Build Your Own Redis in C++)

A high-performance in-memory key-value database built from scratch in C++ following the tutorial [Build Your Own Redis](https://build-your-own.org/redis/).

## Features

- **Event Loop & Non-blocking I/O**: Single-threaded event loop based on `poll()` with non-blocking sockets, handling multiple concurrent connections and pipelining.
- **Custom Progressive Hashtable**: Intrusive hashtable with incremental rehashing (`newer` and `older` tables) to avoid latency spikes during table resizing.
- **Tagged-Union Serialization Protocol**: Supports multiple data types (`nil`, `err`, `str`, `int`, `dbl`, `arr`).
- **Sorted Sets (ZSet)**: Combined AVL tree (for range queries and rank offsets) + intrusive hashtable (for O(1) score lookups).
- **TTL Cache Expiration**: Intrusive binary min-heap with reverse index mapping for active key expiration and timeout calculation.
- **Idle Connection Timeout**: Circular doubly-linked list (`DList`) maintaining LRU order to prune inactive clients.
- **Thread Pool / Asynchronous Deletion**: Worker threads using `pthread` and condition variables to deallocate large data structures in the background without blocking the main event loop.

## Architecture

| Component | Files | Description |
|-----------|-------|-------------|
| **Server & Event Loop** | [src/server.cpp](file:///home/pratik/redis/myredis/src/server.cpp) | Core event loop, request parsing, command routing, timers |
| **Hashtable** | [src/hashtable.h](file:///home/pratik/redis/myredis/src/hashtable.h), [src/hashtable.cpp](file:///home/pratik/redis/myredis/src/hashtable.cpp) | 2-table progressive rehashing dictionary |
| **AVL Tree** | [src/avl.h](file:///home/pratik/redis/myredis/src/avl.h), [src/avl.cpp](file:///home/pratik/redis/myredis/src/avl.cpp) | Balanced binary search tree with subtree rank counting |
| **Sorted Set** | [src/zset.h](file:///home/pratik/redis/myredis/src/zset.h), [src/zset.cpp](file:///home/pratik/redis/myredis/src/zset.cpp) | Dual-indexed ZSet (AVL tree + Hashtable) |
| **Min-Heap** | [src/heap.h](file:///home/pratik/redis/myredis/src/heap.h), [src/heap.cpp](file:///home/pratik/redis/myredis/src/heap.cpp) | Binary min-heap with element back-pointers for TTLs |
| **Doubly-Linked List** | [src/list.h](file:///home/pratik/redis/myredis/src/list.h) | Intrusive circular doubly-linked list for idle timeouts |
| **Thread Pool** | [src/thread_pool.h](file:///home/pratik/redis/myredis/src/thread_pool.h), [src/thread_pool.cpp](file:///home/pratik/redis/myredis/src/thread_pool.cpp) | Producer-consumer work queue for asynchronous deallocation |
| **Client** | [src/client.cpp](file:///home/pratik/redis/myredis/src/client.cpp) | CLI client for executing queries |

## Supported Commands

- `get <key>`: Retrieve string value
- `set <key> <val>`: Set string value
- `del <key>`: Delete key
- `keys`: List all keys in database
- `pexpire <key> <ttl_ms>`: Set TTL on key in milliseconds
- `pttl <key>`: Get remaining TTL in milliseconds (-1 if no TTL, -2 if not found)
- `zadd <zset> <score> <name>`: Add or update element in sorted set
- `zrem <zset> <name>`: Remove element from sorted set
- `zscore <zset> <name>`: Query score of element in sorted set
- `zquery <zset> <score> <name> <offset> <limit>`: Range query sorted set starting from `(score, name)`

## Building & Running

```bash
# Compile both server and client
make

# Start server
./myredis

# Run commands using client
./client set mykey "hello world"
./client get mykey
./client zadd myzset 1.5 item1
./client zadd myzset 3.0 item2
./client zquery myzset 0 "" 0 10
./client pexpire mykey 5000
./client pttl mykey
```
