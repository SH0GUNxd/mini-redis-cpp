#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <algorithm>

Server::Server(int port) 
    : port_(port), 
      server_fd_(-1), 
      thread_pool_(std::thread::hardware_concurrency()) 
{
    load_aof();

    aof_stream_.open("mini-redis.aof", std::ios::app);
    if (!aof_stream_.is_open()) {
        std::cerr << "CRITICAL: Failed to open AOF file for writing!\n";
    }

    // Start the background eviction thread
    eviction_thread_ = std::thread(&Server::eviction_loop, this);
}

Server::~Server() {
    // Gracefully shut down the background thread
    stop_eviction_ = true;
    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }

    if (aof_stream_.is_open()) aof_stream_.close();
    if (server_fd_ != -1) close(server_fd_);
}

// Background thread that sweeps memory every 1 second
void Server::eviction_loop() {
    while (!stop_eviction_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto now = std::chrono::steady_clock::now();

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        for (auto it = store_.begin(); it != store_.end(); ) {
            if (it->second.has_ttl && it->second.expires_at <= now) {
                // Write the deletion to disk so it doesn't revive on crash
                if (aof_stream_.is_open()) {
                    aof_stream_ << "DEL " << it->first << "\n";
                    aof_stream_.flush();
                }
                it = store_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void Server::load_aof() {
    std::ifstream file("mini-redis.aof");
    if (!file.is_open()) return;

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        
        std::istringstream iss(line);
        std::string command;
        iss >> command;

        if (command == "SET") {
            std::string key, value;
            iss >> key;
            std::getline(iss >> std::ws, value);
            store_[key] = {value, {}, false};
        } else if (command == "DEL") {
            std::string key;
            iss >> key;
            store_.erase(key);
        } else if (command == "EXPIRE") {
            std::string key;
            int seconds;
            iss >> key >> seconds;
            auto it = store_.find(key);
            if (it != store_.end()) {
                it->second.has_ttl = true;
                it->second.expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
            }
        }
    }
}

// ... [Networking logic in start() and handle_client() remains EXACTLY the same] ...
void Server::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);
    bind(server_fd_, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd_, 10);
    std::cout << "Mini-Redis listening on port " << port_ << "...\n";

    while (true) {
        struct sockaddr_in client_address{};
        socklen_t client_addr_len = sizeof(client_address);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_address, &client_addr_len);
        if (client_fd < 0) continue;
        thread_pool_.enqueue([this, client_fd]() { this->handle_client(client_fd); });
    }
}

void Server::handle_client(int client_fd) {
    char buffer[1024] = {0};
    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) break;
        buffer[bytes_read] = '\0';
        std::string response = process_command(std::string(buffer));
        write(client_fd, response.c_str(), response.length());
    }
    close(client_fd);
}


std::string Server::process_command(const std::string& input) {
    std::istringstream iss(input);
    std::string command;
    iss >> command;
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "SET") {
        std::string key, value;
        iss >> key;
        std::getline(iss >> std::ws, value); 
        if (!value.empty() && value.back() == '\r') value.pop_back();

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        // Setting a key strips it of any previous TTL
        store_[key] = {value, {}, false};
        
        if (aof_stream_.is_open()) {
            aof_stream_ << "SET " << key << " " << value << "\n";
            aof_stream_.flush(); 
        }
        return "OK\n";

    } else if (command == "EXPIRE") {
        std::string key;
        int seconds;
        iss >> key >> seconds;

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            it->second.has_ttl = true;
            it->second.expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
            
            if (aof_stream_.is_open()) {
                aof_stream_ << "EXPIRE " << key << " " << seconds << "\n";
                aof_stream_.flush();
            }
            return "OK\n";
        }
        return "NULL\n";

    } else if (command == "GET") {
        std::string key;
        iss >> key;
        bool expired = false;

        {
            // First pass: Read lock
            std::shared_lock<std::shared_mutex> lock(store_mutex_);
            auto it = store_.find(key);
            if (it != store_.end()) {
                if (it->second.has_ttl && it->second.expires_at <= std::chrono::steady_clock::now()) {
                    expired = true; // Mark for deletion, but don't delete yet
                } else {
                    return it->second.data + "\n";
                }
            } else {
                return "NULL\n";
            }
        } // Read lock releases here

        // Second pass: Lazy Eviction (Write lock)
        if (expired) {
            std::unique_lock<std::shared_mutex> lock(store_mutex_);
            store_.erase(key);
            if (aof_stream_.is_open()) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return "NULL\n";
        }

    } else if (command == "DEL") {
        std::string key;
        iss >> key;

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        if (store_.erase(key)) {
            if (aof_stream_.is_open()) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return "OK\n";
        }
        return "NULL\n";
    }
    
    return "ERROR: Unknown command\n";
}