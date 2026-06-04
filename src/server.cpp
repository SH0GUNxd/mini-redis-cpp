#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <sys/wait.h>

Server::Server(int port) 
    : port_(port), 
      server_fd_(-1), 
      thread_pool_(std::thread::hardware_concurrency()),
      stop_eviction_(false),
      bgsave_pid_(-1),
      is_replica_(false)
{
    load_aof();
    aof_stream_.open("mini-redis.aof", std::ios::app);
    eviction_thread_ = std::thread(&Server::eviction_loop, this);
}

Server::~Server() {
    stop_eviction_ = true;
    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    
    // Safely wind down the replication thread if it was spawned
    is_replica_ = false;
    if (replica_worker_.joinable()) {
        replica_worker_.join();
    }

    if (aof_stream_.is_open()) {
        aof_stream_.close();
    }
    
    // Safely lock and clear replica connections
    std::unique_lock<std::shared_mutex> lock(replica_mutex_);
    for (int fd : replica_fds_) { 
        if (fd >= 0) close(fd); 
    }
    replica_fds_.clear();

    if (server_fd_ != -1) {
        close(server_fd_);
    }
}

void Server::eviction_loop() {
    while (!stop_eviction_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        pid_t current_bgsave = bgsave_pid_.load();
        if (current_bgsave != -1) {
            int status;
            pid_t result = waitpid(current_bgsave, &status, WNOHANG);
            if (result == current_bgsave) {
                bgsave_pid_.store(-1);
                std::cout << "[BGSAVE] Background snapshot finished successfully.\n";
            }
        }

        auto now = std::chrono::steady_clock::now();
        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        for (auto it = store_.begin(); it != store_.end(); ) {
            if (it->second.has_ttl && it->second.expires_at <= now) {
                if (aof_stream_.is_open()) {
                    aof_stream_ << "DEL " << it->first << "\n";
                    aof_stream_.flush();
                }
                it = store_.erase(it);
            } else { ++it; }
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

std::vector<std::string> Server::parse_resp(const std::string& input) {
    std::vector<std::string> args;
    if (input.empty() || input[0] != '*') return args; 

    size_t pos = 1;
    size_t crlf = input.find("\r\n", pos);
    if (crlf == std::string::npos) return args;

    int num_args = std::stoi(input.substr(pos, crlf - pos));
    pos = crlf + 2;

    for (int i = 0; i < num_args; ++i) {
        if (pos >= input.length() || input[pos] != '$') break;
        pos++;
        crlf = input.find("\r\n", pos);
        if (crlf == std::string::npos) break;
        int len = std::stoi(input.substr(pos, crlf - pos));
        pos = crlf + 2;
        args.push_back(input.substr(pos, len));
        pos += len + 2; 
    }
    return args;
}


void Server::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(static_cast<uint16_t>(port_));
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

void Server::broadcast_to_replicas(const std::string& resp_payload) {
    std::shared_lock<std::shared_mutex> lock(replica_mutex_);
    for (int fd : replica_fds_) {
        // Send the exact RESP command to the replica
        (void)write(fd, resp_payload.c_str(), resp_payload.length());
    }
}

void Server::connect_to_master(std::string host, int port) {
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr);

    if (connect(master_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "Failed to connect to master!\n";
        return;
    }

    std::cout << "Successfully connected to master at " << host << ":" << port << "\n";
    
    // Handshake: Tell the master we are a replica
    std::string handshake = "*1\r\n$12\r\nI_AM_REPLICA\r\n";
    (void)write(master_fd, handshake.c_str(), handshake.length());

    // Enter a continuous loop to receive and apply broadcasted commands
    char buffer[4096] = {0};
    while (is_replica_) {
        ssize_t bytes_read = read(master_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            std::cout << "Master disconnected.\n";
            break;
        }
        buffer[bytes_read] = '\0';
        
        std::vector<std::string> args = parse_resp(std::string(buffer));
        if (!args.empty()) {
            process_command(args, -1); // Process silently, don't write back to master
        }
    }
    close(master_fd);
}

void Server::handle_client(int client_fd) {
    char buffer[4096] = {0}; 
    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) break;
        buffer[bytes_read] = '\0';
        
        std::vector<std::string> args = parse_resp(std::string(buffer));
        
        std::string response;
        if (args.empty()) {
            response = "-ERR Protocol error\r\n";
        } else {
            // Pass the client_fd so the parser can track replicas
            response = process_command(args, client_fd); 
            
            // If the command mutates state, reconstruct the RESP string and broadcast it
            std::string cmd = args[0];
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
            if (cmd == "SET" || cmd == "DEL" || cmd == "EXPIRE") {
                broadcast_to_replicas(std::string(buffer)); 
            }
        }
        
        // Only write back to normal clients. Replicas just stay connected silently.
        if (!response.empty()) {
            (void)write(client_fd, response.c_str(), response.length());
        }
    }
    
    // If a client disconnects, check if it was a replica and remove it
    std::unique_lock<std::shared_mutex> lock(replica_mutex_);
    replica_fds_.erase(std::remove(replica_fds_.begin(), replica_fds_.end(), client_fd), replica_fds_.end());
    
    close(client_fd);
}

std::string Server::process_command(const std::vector<std::string>& args, int client_fd) {
    std::string command = args[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "PING") return "+PONG\r\n"; 
    
    else if (command == "I_AM_REPLICA") {
        std::unique_lock<std::shared_mutex> lock(replica_mutex_);
        if (client_fd != -1) replica_fds_.push_back(client_fd);
        std::cout << "Registered new replica connection.\n";
        return ""; // Return empty so we don't close the socket or reply
    }
    else if (command == "REPLICAOF") {
        if (args.size() != 3) return "-ERR syntax error\r\n";
        std::string host = args[1];
        int port = std::stoi(args[2]);
        
        is_replica_ = true;
        replica_worker_ = std::thread(&Server::connect_to_master, this, host, port);
        return "+OK\r\n";
    }

    else if (command == "BGSAVE") {
        if (bgsave_pid_.load() != -1) return "-ERR Background save already in progress\r\n";
        store_mutex_.lock(); 
        pid_t pid = fork();

        if (pid == 0) {
            std::ofstream rdb("dump.rdb", std::ios::trunc);
            for (const auto& [k, v] : store_) rdb << k << " " << v.data << "\n";
            rdb.flush();
            rdb.close();
            _exit(0); 
        } else if (pid > 0) {
            bgsave_pid_.store(pid);
            store_mutex_.unlock();
            return "+Background saving started\r\n";
        } else {
            store_mutex_.unlock();
            return "-ERR Failed to create background process\r\n";
        }
    }
    
    else if (command == "SET") {
        if (args.size() < 3) return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];
        std::string value = args[2];

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        store_[key] = {value, {}, false};
        
        if (aof_stream_.is_open() && !is_replica_) {
            aof_stream_ << "SET " << key << " " << value << "\n";
            aof_stream_.flush(); 
        }
        return "+OK\r\n";

    } 
    else if (command == "EXPIRE") {
        if (args.size() != 3) return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];
        int seconds = std::stoi(args[2]);

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            it->second.has_ttl = true;
            it->second.expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
            if (aof_stream_.is_open() && !is_replica_) {
                aof_stream_ << "EXPIRE " << key << " " << seconds << "\n";
                aof_stream_.flush();
            }
            return ":1\r\n";
        }
        return ":0\r\n";

    } 
    else if (command == "GET") {
        if (args.size() != 2) return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];
        bool expired = false;

        {
            std::shared_lock<std::shared_mutex> lock(store_mutex_);
            auto it = store_.find(key);
            if (it != store_.end()) {
                if (it->second.has_ttl && it->second.expires_at <= std::chrono::steady_clock::now()) {
                    expired = true; 
                } else {
                    return "$" + std::to_string(it->second.data.length()) + "\r\n" + it->second.data + "\r\n";
                }
            } else {
                return "$-1\r\n"; 
            }
        } 

        if (expired) {
            std::unique_lock<std::shared_mutex> lock(store_mutex_);
            store_.erase(key);
            if (aof_stream_.is_open() && !is_replica_) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return "$-1\r\n"; 
        }
    } 
    else if (command == "DEL") {
        if (args.size() != 2) return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        if (store_.erase(key)) {
            if (aof_stream_.is_open() && !is_replica_) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return ":1\r\n";
        }
        return ":0\r\n"; 
    }
    
    return "-ERR unknown command\r\n";
}