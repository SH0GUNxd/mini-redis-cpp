# Mini-Redis

A multithreaded, in-memory key-value store built from scratch in C++20, implementing a subset of the Redis protocol.

Built to explore systems-level backend engineering directly with OS primitives - no frameworks, no external dependencies beyond the C++ standard library. The server speaks real RESP, so it works with `redis-cli` and any standard Redis client out of the box.

---

## Architecture

### RESP Protocol Parser

Implements the REdis Serialization Protocol (RESP) with a stateful accumulation buffer per client connection, handling partial TCP reads correctly. Commands are parsed as binary-safe bulk string arrays.

### Thread Pool

Incoming connections are dispatched to a fixed-size worker pool backed by `std::condition_variable` and `std::mutex`. Thread count defaults to `std::thread::hardware_concurrency()`, avoiding per-connection thread spawning overhead.

### Reader-Writer Locking

Uses `std::shared_mutex` to allow concurrent `GET` operations while serializing writes (`SET`, `DEL`, `EXPIRE`). AOF writes are protected by a separate `std::mutex` to avoid contention with store operations.

### Append-Only File (AOF) Persistence

Every mutating command is appended to `mini-redis.aof` and flushed to disk synchronously. On startup, the AOF is replayed to reconstruct the full database state.

### BGSAVE Snapshotting

`BGSAVE` forks a child process using `fork()`. The child serializes the current store to `dump.rdb` and exits, while the parent continues serving requests. The eviction thread reaps the child with `waitpid(WNOHANG)` to avoid zombie processes.

### TTL Eviction

Two-pronged expiration strategy:

- **Lazy eviction** - expiry is checked on `GET`; expired keys are deleted on access.
- **Active eviction** - a dedicated background thread sweeps the store every second and removes expired keys proactively.

### Master-Replica Replication

A running instance can be turned into a replica with `REPLICAOF <host> <port>`. The replica connects to the master, registers itself, and receives a live stream of mutating commands. The master broadcasts `SET`, `DEL`, and `EXPIRE` to all connected replicas after each write.

### Containerized Deployment

Multi-stage Docker build: a builder stage compiles the binary with GCC, a minimal runner stage copies only the binary. Docker Compose orchestrates the server and benchmark containers.

---

## Tech Stack

| Category    | Technologies                                           |
|-------------|--------------------------------------------------------|
| Server      | C++20, POSIX Sockets, CMake                            |
| Concurrency | `std::thread`, `std::shared_mutex`, Custom Thread Pool |
| Persistence | AOF (append-only log), RDB snapshot via `fork()`       |
| Testing     | Python 3, `pytest`, `socket`                           |
| DevOps      | Docker, Docker Compose, GitHub Actions                 |

---

## Project Structure

```text
mini-redis/
├── CMakeLists.txt         # Build configuration
├── Dockerfile             # Multi-stage builder and runtime image
├── docker-compose.yml     # Infrastructure orchestration
├── src/
│   ├── main.cpp           # Entry point
│   ├── server.hpp         # Declarations
│   ├── server.cpp         # Socket, RESP, storage, AOF, replication
│   └── thread_pool.hpp    # Worker pool implementation
├── client/
│   └── benchmark.py       # Async RESP load-testing utility
└── tests/
    └── test_server.py     # Network-level integration tests
```

---

## Getting Started

### Option 1: Docker (Recommended)

```bash
docker compose up -d --build server
```

To also run the benchmark:

```bash
docker compose up --build
```

### Option 2: Local Build (Linux/macOS)

Requirements: CMake, Make, GCC 11+ or Clang 13+

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
./mini-redis
```

> Debug builds enable AddressSanitizer and UBSan automatically via CMake.

---

## Connecting

Mini-Redis speaks RESP, so the standard Redis CLI works:

```bash
# Ubuntu/Debian
sudo apt install redis-tools

# macOS
brew install redis

redis-cli -p 6379
```

---

## Supported Commands

| Command                  | Description                                          |
|--------------------------|------------------------------------------------------|
| `PING`                   | Liveness check. Returns `PONG`.                      |
| `SET <key> <value>`      | Store a key-value pair.                              |
| `GET <key>`              | Retrieve a value. Returns null if missing or expired.|
| `DEL <key>`              | Delete a key. Returns count of deleted keys.         |
| `EXPIRE <key> <seconds>` | Set a TTL on an existing key.                        |
| `BGSAVE`                 | Fork a child process to write `dump.rdb` to disk.   |
| `REPLICAOF <host> <port>`| Turn this instance into a replica of a master.       |

---

## Example Session

```
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET user:1 "John Doe"
OK

127.0.0.1:6379> EXPIRE user:1 5
(integer) 1

127.0.0.1:6379> GET user:1
"John Doe"

# After TTL expires:
127.0.0.1:6379> GET user:1
(nil)

127.0.0.1:6379> BGSAVE
Background saving started
```

---

## Testing

Integration tests validate protocol correctness, TTL expiration, persistence, and argument validation over a real TCP connection.

```bash
pip install pytest
pytest tests/ -v
```

CI runs the full test suite on every push via GitHub Actions, building the server in a Docker container and running pytest against the exposed port.

---

## Benchmarking

```bash
python3 client/benchmark.py
```

Default configuration: 50 concurrent connections, 100,000 requests, RESP-compliant. Reports throughput and latency.

**Results on Windows/WSL2 (Docker):**

| Pass                         | Throughput     | Avg latency | p50       | p95       | p99       |
|------------------------------|----------------|-------------|-----------|-----------|-----------|
| Latency (per-request drain)  | 9,499 ops/sec  | 3.191 ms    | 1.532 ms  | 1.842 ms  | 2.132 ms  |
| Throughput (pipeline size=32)| 9,090 ops/sec  |            |          |          |          |

> Measured over 100,000 requests (80% GET / 20% SET) via Docker on Windows. The pipelined pass shows no throughput gain over the latency pass, indicating the bottleneck is server-side (mutex contention) rather than client-side flushing - as expected under WSL2. Native Linux bare-metal results would be significantly higher.

---

## Known Limitations

- **No partial-write recovery on AOF** - if the process crashes mid-append, the trailing entry may be malformed on replay.
- **Replication is fire-and-forget** - the master does not retry failed replica writes or track replication offset.
- **Single-instance replication only** - `REPLICAOF` can only be called once per instance; chained replication is not supported.
- **No AUTH, TLS, or access control** - not intended for production use.

---

## License

Educational and portfolio use. Fork freely.
