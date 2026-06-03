#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sstream>
#include <algorithm>

Server::Server(int port) : port_(port), server_fd_(-1) {}
Server::~Server() { if (server_fd_ != -1) close(server_fd_); }

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
    
    while (true) {
        struct sockaddr_in client_address{};
        socklen_t client_addr_len = sizeof(client_address);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_address, &client_addr_len);
        if (client_fd >= 0) handle_client(client_fd);
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
        if (key.empty() || value.empty()) return "ERROR\n";
        store_[key] = value;
        return "OK\n";
    } else if (command == "GET") {
        std::string key;
        iss >> key;
        auto it = store_.find(key);
        return (it != store_.end()) ? it->second + "\n" : "NULL\n";
    } else if (command == "DEL") {
        std::string key;
        iss >> key;
        return store_.erase(key) ? "OK\n" : "NULL\n";
    }
    return "ERROR\n";
}