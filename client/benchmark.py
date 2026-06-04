import asyncio
import random
import string
import time
import os

# Configuration: Use REDIS_HOST from environment, default to 127.0.0.1
HOST = os.getenv("REDIS_HOST", "127.0.0.1")
PORT = 6379
TOTAL_REQUESTS = 100000
CONCURRENCY = 50 

def generate_random_string(length=8):
    """Generates a random string for keys and values."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def encode_resp(*args) -> bytes:
    """Helper to convert standard arguments into a RESP array."""
    resp = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        resp += f"${len(arg_str)}\r\n{arg_str}\r\n"
    return resp.encode()

async def worker(worker_id: int, requests_per_worker: int, latencies: list):
    """A single concurrent worker hitting the C++ server."""
    try:
        # Open a distinct TCP connection for this worker
        reader, writer = await asyncio.open_connection(HOST, PORT)
    except Exception as e:
        print(f"Worker {worker_id} failed to connect: {e}")
        return

    for _ in range(requests_per_worker):
        # 80% GET requests, 20% SET requests to mimic production cache distribution
        is_set = random.random() < 0.2
        key = f"key:{random.randint(1, 1000)}"
        
        # USE RESP ENCODING HERE
        if is_set:
            val = generate_random_string()
            payload = encode_resp("SET", key, val)
        else:
            payload = encode_resp("GET", key)

        # Start timer for latency tracking
        start_time = time.perf_counter()
        
        # Send payload
        writer.write(payload)
        await writer.drain()
        
        # Read the RESP response
        response = await reader.readline()
        
        # If it's a bulk string, we need to read the actual data line too
        if response.startswith(b'$') and response != b'$-1\r\n':
            await reader.readline()
            
        # End timer
        end_time = time.perf_counter()
        latencies.append(end_time - start_time)

    # Clean up connection
    writer.close()
    await writer.wait_closed()

async def main():
    print(f"Starting benchmark against {HOST}:{PORT}")
    print(f"Total Requests: {TOTAL_REQUESTS} | Concurrency: {CONCURRENCY}")
    
    requests_per_worker = TOTAL_REQUESTS // CONCURRENCY
    latencies = []
    
    start_total_time = time.perf_counter()
    
    # Spawn concurrent workers
    tasks = [
        worker(i, requests_per_worker, latencies) 
        for i in range(CONCURRENCY)
    ]
    await asyncio.gather(*tasks)
    
    end_total_time = time.perf_counter()
    
    # Calculate performance metrics
    total_duration = end_total_time - start_total_time
    ops_per_sec = len(latencies) / total_duration
    avg_latency_ms = (sum(latencies) / len(latencies)) * 1000 if latencies else 0

    sorted_latencies = sorted(latencies)
    p50 = sorted_latencies[int(len(sorted_latencies) * 0.50)] * 1000 if latencies else 0
    p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)] * 1000 if latencies else 0
    p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)] * 1000 if latencies else 0

    print(f"\n--- Benchmark Results ---")
    print(f"Total time:        {total_duration:.2f}s")
    print(f"Requests done:     {len(latencies)}")
    print(f"Throughput:        {ops_per_sec:.0f} ops/sec")
    print(f"Avg latency:       {avg_latency_ms:.3f} ms")
    print(f"p50 latency:       {p50:.3f} ms")
    print(f"p95 latency:       {p95:.3f} ms")
    print(f"p99 latency:       {p99:.3f} ms")

if __name__ == "__main__":
    asyncio.run(main())