# Mini-Redis

A high-performance, multithreaded, in-memory key-value store built from scratch in Modern C++20.

This project was developed to explore systems-level backend engineering, bypassing heavy frameworks to work directly with OS-level primitives. It acts as a fully functional Redis clone, complete with a custom POSIX socket TCP server, thread-safe memory management, durable disk persistence, time-to-live (TTL) eviction, and full compliance with the binary-safe RESP protocol.

---

## Core Architecture & Features

### RESP Protocol Compliance

Implements a binary-safe REdis Serialization Protocol (RESP) parser. The server can be queried using the official `redis-cli` or any standard Redis client library in Python, Node.js, Go, Java, and more.

### Thread Pool Concurrency

Prevents expensive thread-spawning overhead by utilizing a pre-allocated worker pool backed by `std::condition_variable` and `std::mutex`.

### Lock-Free Reads

Uses C++20 `std::shared_mutex` (reader-writer locks) to allow unlimited concurrent `GET` operations while maintaining strict exclusivity for mutating commands such as `SET`, `DEL`, and `EXPIRE`.

### Disk Persistence (AOF)

Implements an Append-Only File (AOF) persistence layer for crash recovery. Every mutating operation is appended to disk and replayed during startup to reconstruct the exact database state.

### Time-To-Live (TTL) Eviction

Supports Redis-style expiration through the `EXPIRE` command using `std::chrono`.

The system employs a dual-eviction strategy:

* **Lazy Eviction** – Expiration is checked during reads. Expired keys are deleted on access.
* **Active Eviction** – A dedicated background thread periodically sweeps memory to remove expired keys proactively without impacting request handling.

### Containerized Infrastructure

Fully containerized with Docker and Docker Compose using multi-stage builds for a minimal and secure production image.

### Continuous Integration

Automated network-level integration tests executed through GitHub Actions to ensure protocol correctness, persistence reliability, and thread safety.

---

## Tech Stack

| Category              | Technologies                                           |
| --------------------- | ------------------------------------------------------ |
| Server                | C++20, POSIX Sockets, CMake                            |
| Concurrency           | `std::thread`, `std::shared_mutex`, Custom Thread Pool |
| Client / Benchmarking | Python 3.10, `asyncio`, `socket`                       |
| Testing               | `pytest`                                               |
| DevOps                | Docker, Docker Compose, GitHub Actions                 |

---

## Project Structure

```text
mini-redis/
├── CMakeLists.txt         # Build configuration
├── docker-compose.yml     # Infrastructure orchestration
├── Dockerfile             # Multi-stage builder and runtime image
├── src/
│   ├── main.cpp           # Application entry point
│   ├── server.hpp         # Server and protocol declarations
│   ├── server.cpp         # Socket, AOF, RESP, and storage logic
│   └── thread_pool.hpp    # Concurrent worker pool implementation
├── client/
│   └── benchmark.py       # Async RESP load-testing utility
└── tests/
    └── test_server.py     # Network-level integration tests
```

---

## Getting Started

You can run the project locally on Linux/macOS or through Docker.

### Option 1: Docker (Recommended)

Ensure Docker and Docker Compose are installed.

```bash
# Build and launch the server + benchmark container
docker-compose up --build
```

**Note:** This command starts the C++ server and automatically runs the benchmark against it through Docker's internal bridge network.

To run only the server in the background:

```bash
docker-compose up -d server
```

---

### Option 2: Local Build (Linux/macOS)

Requirements:

* CMake
* Make
* GCC 11+ or Clang 13+
* C++20-compatible compiler

Build the project:

```bash
mkdir build
cd build

cmake ..
make
```

Run the server:

```bash
./mini-redis
```

---

## Interacting with the Server

Because Mini-Redis implements the official RESP protocol, you can connect using the standard Redis CLI.

Install Redis CLI:

### Ubuntu / Debian

```bash
sudo apt install redis-tools
```

### macOS

```bash
brew install redis
```

Connect:

```bash
redis-cli -p 6379
```

---

## Supported Commands

| Command                  | Description                             |
| ------------------------ | --------------------------------------- |
| `PING`                   | Checks server liveness. Returns `PONG`. |
| `SET <key> <value>`      | Stores a binary-safe key-value pair.    |
| `GET <key>`              | Retrieves a value by key.               |
| `DEL <key>`              | Deletes a key-value pair.               |
| `EXPIRE <key> <seconds>` | Sets a TTL on a key.                    |

---

## Example Session

```text
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET user:1 "John Doe"
OK

127.0.0.1:6379> EXPIRE user:1 5
(integer) 1

127.0.0.1:6379> GET user:1
"John Doe"

127.0.0.1:6379> GET user:1
(nil)
```

*(The final GET is executed after the TTL expires.)*

---

## Benchmarking

To evaluate throughput and latency on your hardware, run the asynchronous benchmark utility while the server is running.

```bash
python3 client/benchmark.py
```

Default benchmark configuration:

* 50 concurrent TCP connections
* 100,000 requests
* RESP-compliant request generation
* Latency and throughput reporting

---

## Testing

Integration tests are written with `pytest` and validate:

* RESP protocol compliance
* Network I/O correctness
* Thread-safe concurrent operations
* TTL expiration behavior
* Persistence and recovery

Install dependencies:

```bash
pip install pytest
```

Run the test suite:

```bash
pytest tests/ -v
```

---

## Design Highlights

Mini-Redis focuses on demonstrating low-level systems programming concepts commonly used in high-performance backend infrastructure:

* TCP networking using POSIX sockets
* Concurrent request processing with thread pools
* Reader-writer locking using `std::shared_mutex`
* Durable append-only persistence
* TTL-based memory management
* RESP protocol implementation from scratch
* Containerized deployment pipelines
* Automated integration testing

The project intentionally avoids external frameworks and databases to expose the underlying mechanics behind modern in-memory data stores such as Redis.

---

## License

This project is intended for educational and portfolio purposes. Feel free to fork, modify, and extend it for your own learning.
