import socket
import pytest
import time
import os

# Use environment variable for host, defaulting to localhost for local runs
HOST = os.getenv("REDIS_HOST", "127.0.0.1")
PORT = 6379

@pytest.fixture
def redis_client():
    """
    Pytest fixture to setup and teardown a socket connection 
    for each test automatically.
    """
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Add a slight retry logic to ensure the container is ready in CI
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

def send_command(client, command: str) -> str:
    """Helper to send a command and read the response."""
    client.sendall(command.encode())
    return client.recv(1024).decode()

def test_set_and_get(redis_client):
    """Test basic SET and GET functionality."""
    # 1. SET a key
    resp = send_command(redis_client, "SET user:99 Alice\n")
    assert resp == "OK\n"

    # 2. GET the key
    resp = send_command(redis_client, "GET user:99\n")
    assert resp == "Alice\n"

def test_get_nonexistent_key(redis_client):
    """Test GETting a key that does not exist."""
    resp = send_command(redis_client, "GET ghost_key\n")
    assert resp == "NULL\n"

def test_delete_key(redis_client):
    """Test DEL functionality."""
    # Setup
    send_command(redis_client, "SET temp_key 12345\n")
    
    # Delete
    resp = send_command(redis_client, "DEL temp_key\n")
    assert resp == "OK\n"
    
    # Verify it is gone
    resp = send_command(redis_client, "GET temp_key\n")
    assert resp == "NULL\n"

def test_delete_nonexistent_key(redis_client):
    """Test deleting a key that doesn't exist."""
    resp = send_command(redis_client, "DEL ghost_key\n")
    assert resp == "NULL\n"

def test_case_insensitivity(redis_client):
    """Test that commands work regardless of casing."""
    resp1 = send_command(redis_client, "set mixed_case Val\n")
    assert resp1 == "OK\n"
    
    resp2 = send_command(redis_client, "gEt mixed_case\n")
    assert resp2 == "Val\n"