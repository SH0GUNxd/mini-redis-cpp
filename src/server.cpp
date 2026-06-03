#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <algorithm>

// Initialize thread pool with the number of CPU cores available
Server::Server(int port) 
    : port_(port), 
      server_fd_(-1), 
      thread_pool_(std::thread::hardware_concurrency()) {}

Server::~Server() {
    if (server_fd_ != -1) {
        close(server_fd_);
        std::cout << "Server socket closed.\n";
    }
}

void Server::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        perror("Failed to create socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd_, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    std::cout << "Mini-Redis listening on port " << port_ 
              << " with " << std::thread::hardware_concurrency() << " worker threads...\n";

    while (true) {
        struct sockaddr_in client_address{};
        socklen_t client_addr_len = sizeof(client_address);
        
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_address, &client_addr_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }

        // Dispatch the client to the thread pool instead of blocking the main thread
        thread_pool_.enqueue([this, client_fd]() {
            this->handle_client(client_fd);
        });
    }
}

void Server::handle_client(int client_fd) {
    char buffer[1024] = {0};
    
    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        
        if (bytes_read <= 0) {
            break; // Client disconnected quietly
        }

        buffer[bytes_read] = '\0';
        std::string raw_input(buffer);

        std::string response = process_command(raw_input);
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

        // EXCLUSIVE LOCK: Only one thread can write at a time
        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        store_[key] = value;
        return "OK\n";

    } else if (command == "GET") {
        std::string key;
        iss >> key;

        // SHARED LOCK: Multiple threads can read simultaneously
        std::shared_lock<std::shared_mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            return it->second + "\n";
        }
        return "NULL\n";

    } else if (command == "DEL") {
        std::string key;
        iss >> key;

        // EXCLUSIVE LOCK: Only one thread can write/delete at a time
        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        if (store_.erase(key)) {
            return "OK\n";
        }
        return "NULL\n";

    } else {
        return "ERROR: Unknown command\n";
    }
}