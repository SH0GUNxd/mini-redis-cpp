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
        
        if is_set:
            val = generate_random_string()
            command = f"SET {key} {val}\n"
        else:
            command = f"GET {key}\n"

        # Start timer for latency tracking
        start_time = time.perf_counter()
        
        # Send payload
        writer.write(command.encode())
        await writer.drain()
        
        # Read response line
        response = await reader.readline()
        
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
    
    print("\n" + "="*30)
    print("BENCHMARK RESULTS")
    print("="*30)
    print(f"Total Time Taken: {total_duration:.4f} seconds")
    print(f"Successful Req:   {len(latencies)}")
    print(f"Throughput:       {ops_per_sec:.2f} Ops/sec")
    print(f"Avg Latency:      {avg_latency_ms:.4f} ms")
    print("="*30)

if __name__ == "__main__":
    # Run the async event loop
    asyncio.run(main())