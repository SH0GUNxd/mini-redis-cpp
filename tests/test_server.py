import socket
import pytest
import time
import os

HOST = os.getenv("REDIS_HOST", "127.0.0.1")
PORT = 6379

def encode_resp(*args) -> bytes:
    """Helper to convert standard arguments into a RESP array."""
    resp = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        resp += f"${len(arg_str)}\r\n{arg_str}\r\n"
    return resp.encode()

@pytest.fixture
def redis_client():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    retries = 5
    while retries > 0:
        try:
            client.connect((HOST, PORT))
            break
        except ConnectionRefusedError:
            time.sleep(1)
            retries -= 1
    yield client
    client.close()

def send_cmd(client, *args) -> str:
    """Encodes the command to RESP, sends it, and decodes the response."""
    client.sendall(encode_resp(*args))
    return client.recv(1024).decode()

def test_ping(redis_client):
    """Test the connection liveness command."""
    assert send_cmd(redis_client, "PING") == "+PONG\r\n"

def test_set_and_get(redis_client):
    """Test basic SET and GET with RESP Bulk Strings."""
    # SET returns a Simple String
    assert send_cmd(redis_client, "SET", "user:99", "Alice") == "+OK\r\n"
    
    # GET returns a Bulk String: $<length>\r\n<data>\r\n
    assert send_cmd(redis_client, "GET", "user:99") == "$5\r\nAlice\r\n"

def test_get_nonexistent_key(redis_client):
    """Test GETting a missing key returns a RESP Null."""
    assert send_cmd(redis_client, "GET", "ghost_key") == "$-1\r\n"

def test_delete_key(redis_client):
    """Test DEL returns RESP Integers representing deleted count."""
    send_cmd(redis_client, "SET", "temp", "data")
    
    # Successfully deleted 1 key
    assert send_cmd(redis_client, "DEL", "temp") == ":1\r\n"
    # Key is already gone, deleted 0 keys
    assert send_cmd(redis_client, "DEL", "temp") == ":0\r\n"

def test_expire_and_ttl(redis_client):
    """Test that background/lazy eviction correctly deletes expired keys."""
    send_cmd(redis_client, "SET", "milk", "whole")
    
    # Set a 1-second TTL. Expect :1 (success)
    assert send_cmd(redis_client, "EXPIRE", "milk", "1") == ":1\r\n"
    
    # It should still exist immediately after
    assert send_cmd(redis_client, "GET", "milk") == "$5\r\nwhole\r\n"
    
    # Wait for the TTL to pass
    time.sleep(1.2)
    
    # It should now be evicted and return Null
    assert send_cmd(redis_client, "GET", "milk") == "$-1\r\n"

def test_argument_validation(redis_client):
    """Test that the server rejects malformed RESP requests gracefully."""
    # SET requires 3 arguments, we only send 2
    resp = send_cmd(redis_client, "SET", "only_key")
    assert resp.startswith("-ERR")
    
def test_bgsave_creates_snapshot(redis_client):
    """Test that BGSAVE forks a process and writes dump.rdb to disk."""
    # 1. Put some data in the database
    send_cmd(redis_client, "SET", "backup_key", "important_data")
    
    # 2. Ensure any old dump file is removed before we test
    if os.path.exists("dump.rdb"):
        os.remove("dump.rdb")
        
    # 3. Trigger the background save
    response = send_cmd(redis_client, "BGSAVE")
    assert "+Background saving started" in response
    
    # 4. Wait a moment for the C++ child process to write to disk and exit
    time.sleep(1.5)
    
    # 5. Verify the OS actually created the file
    assert os.path.exists("dump.rdb"), "BGSAVE failed to create dump.rdb!"