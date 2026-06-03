# Mini-Redis

A high-performance, multithreaded, in-memory key-value store built from scratch in Modern C++20.

This project was developed to explore systems-level backend engineering by working directly with OS-level primitives rather than relying on heavy frameworks. It features a custom POSIX socket TCP server, a thread-safe command parser, and an asynchronous Python benchmarking suite capable of handling tens of thousands of operations per second.

---

## Core Architecture & Features

* **Raw POSIX Networking** – Built directly on Linux socket APIs (`sys/socket.h`) for low-level TCP connection handling.
* **Thread Pool Concurrency** – Eliminates thread-spawning overhead through a pre-allocated worker pool powered by `std::condition_variable` and `std::mutex`.
* **Lock-Free Reads** – Uses `std::shared_mutex` to enable unlimited concurrent `GET` operations while maintaining exclusive access for `SET` and `DEL`.
* **In-Memory Storage** – Provides O(1) average lookup performance using `std::unordered_map`.
* **Async Benchmarking** – Includes a Python `asyncio` benchmarking client for high-concurrency load testing and latency measurement.
* **Containerized Infrastructure** – Uses Docker and Docker Compose with multi-stage builds for compact production images.
* **Continuous Integration** – Automated integration testing with `pytest` and GitHub Actions.

---

## Tech Stack

### Server

* C++20
* POSIX Sockets
* CMake

### Concurrency

* `std::thread`
* `std::shared_mutex`
* Custom Thread Pool

### Client & Benchmarking

* Python 3.10+
* `asyncio`
* `socket`

### Testing

* `pytest`

### DevOps

* Docker
* Docker Compose
* GitHub Actions

---

## Project Structure

```text
mini-redis/
├── CMakeLists.txt         # Build configuration
├── docker-compose.yml     # Infrastructure orchestration
├── Dockerfile             # Multi-stage C++ builder and runner
├── src/
│   ├── main.cpp           # Application entry point
│   ├── server.hpp         # Server and protocol declarations
│   ├── server.cpp         # Socket management and lock logic
│   └── thread_pool.hpp    # Concurrent worker pool implementation
├── client/
│   └── benchmark.py       # High-throughput async load tester
└── tests/
    └── test_server.py     # Network-level integration tests
```

---

## Getting Started

You can run Mini-Redis locally on Linux/macOS or inside Docker.

### Option 1: Docker (Recommended)

Ensure Docker and Docker Compose are installed.

```bash
# Build the images and start the server + benchmark
docker-compose up --build
```

> **Note:** This automatically starts the C++ server and runs the Python benchmark against it over an internal Docker bridge network.

To run only the server in the background:

```bash
docker-compose up -d server
```

---

### Option 2: Local Build (Linux/macOS)

Requirements:

* CMake
* Make
* GCC or Clang with C++20 support

Build and run:

```bash
# Create build directory
mkdir build && cd build

# Generate build files
cmake ..

# Compile
make

# Start server
./mini-redis
```

---

## Interacting with the Server

Once the server is running, connect using Netcat or Telnet:

```bash
nc localhost 6379
```

### Supported Commands

#### SET

Stores a key-value pair.

```text
SET <key> <value>
```

Values may contain spaces.

#### GET

Retrieves a value by key.

```text
GET <key>
```

Returns `NULL` if the key does not exist.

#### DEL

Deletes a key-value pair.

```text
DEL <key>
```

Returns `OK` if deleted, otherwise `NULL`.

### Example Session

```text
SET user:1 John Doe
OK

GET user:1
John Doe

DEL user:1
OK

GET user:1
NULL
```

---

## Benchmarking

To measure throughput and latency, run the asynchronous benchmark client while the server is running.

```bash
# Install requirements (if not using Docker)
python3 -m pip install asyncio

# Run benchmark
python3 client/benchmark.py
```

Default benchmark configuration:

* 50 concurrent connections
* 100,000 requests

---

## Testing

Integration tests verify protocol correctness and network I/O behavior.

Install pytest:

```bash
pip install pytest
```

Run the test suite:

```bash
pytest tests/ -v
```

> Ensure the Mini-Redis server is running before executing the tests.

---

## Contributing

This repository was created as a systems programming and backend engineering learning project, but contributions and experimentation are welcome.

Potential enhancements include:

* Time-to-Live (TTL) expiration support
* Redis Serialization Protocol (RESP) compatibility
* Append-Only File (AOF) persistence
* Snapshot-based persistence
* Replication support
* Metrics and observability tooling

Feel free to fork the project, submit pull requests, or use it as a foundation for exploring distributed systems and high-performance networking in C++.
