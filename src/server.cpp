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
    // 1. Load existing data from disk before doing anything else
    load_aof();

    // 2. Open the AOF file in append mode. It stays open for the life of the server.
    aof_stream_.open("mini-redis.aof", std::ios::app);
    if (!aof_stream_.is_open()) {
        std::cerr << "CRITICAL: Failed to open AOF file for writing!\n";
    }
}

Server::~Server() {
    if (aof_stream_.is_open()) {
        aof_stream_.close();
    }
    if (server_fd_ != -1) {
        close(server_fd_);
        std::cout << "Server socket closed.\n";
    }
}

void Server::load_aof() {
    std::ifstream file("mini-redis.aof");
    if (!file.is_open()) {
        std::cout << "No existing AOF file found. Starting fresh.\n";
        return;
    }

    std::string line;
    int ops_restored = 0;
    
    // Read the file line by line and reconstruct the map
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        
        std::istringstream iss(line);
        std::string command;
        iss >> command;

        if (command == "SET") {
            std::string key, value;
            iss >> key;
            std::getline(iss >> std::ws, value);
            store_[key] = value;
            ops_restored++;
        } else if (command == "DEL") {
            std::string key;
            iss >> key;
            store_.erase(key);
            ops_restored++;
        }
    }
    std::cout << "AOF Loaded: " << ops_restored << " operations restored into memory.\n";
}

void Server::start() {
    // ... [Networking logic remains exactly the same as Phase 3]
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) exit(EXIT_FAILURE);

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    bind(server_fd_, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd_, 10);

    std::cout << "Mini-Redis listening on port " << port_ 
              << " with " << std::thread::hardware_concurrency() << " worker threads...\n";

    while (true) {
        struct sockaddr_in client_address{};
        socklen_t client_addr_len = sizeof(client_address);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_address, &client_addr_len);
        if (client_fd < 0) continue;

        thread_pool_.enqueue([this, client_fd]() {
            this->handle_client(client_fd);
        });
    }
}

void Server::handle_client(int client_fd) {
    // ... [Client loop remains exactly the same as Phase 3]
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
        
        if (!value.empty() && value.back() == '\r') {
            value.pop_back();
        }

        if (key.empty() || value.empty()) {
            return "ERROR: SET requires a key and a value\n";
        }

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        store_[key] = value;
        
        // PERSIST TO DISK
        if (aof_stream_.is_open()) {
            aof_stream_ << "SET " << key << " " << value << "\n";
            aof_stream_.flush(); // Force write to OS buffer
        }
        
        return "OK\n";

    } else if (command == "GET") {
        std::string key;
        iss >> key;

        std::shared_lock<std::shared_mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            return it->second + "\n";
        }
        return "NULL\n";

    } else if (command == "DEL") {
        std::string key;
        iss >> key;

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        if (store_.erase(key)) {
            
            // PERSIST TO DISK
            if (aof_stream_.is_open()) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush(); // Force write to OS buffer
            }
            
            return "OK\n";
        }
        return "NULL\n";

    } else {
        return "ERROR: Unknown command\n";
    }
}