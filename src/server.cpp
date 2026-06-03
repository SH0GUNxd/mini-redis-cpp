#include "server.hpp"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <sys/wait.h> // Required for waitpid()

Server::Server(int port) : port_(port), server_fd_(-1), thread_pool_(std::thread::hardware_concurrency()) {
    load_aof();
    aof_stream_.open("mini-redis.aof", std::ios::app);
    eviction_thread_ = std::thread(&Server::eviction_loop, this);
}

Server::~Server() {
    stop_eviction_ = true;
    if (eviction_thread_.joinable()) eviction_thread_.join();
    if (aof_stream_.is_open()) aof_stream_.close();
    if (server_fd_ != -1) close(server_fd_);
}

void Server::eviction_loop() {
    while (!stop_eviction_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        pid_t current_bgsave = bgsave_pid_.load();
        if (current_bgsave != -1) {
            int status;
            // WNOHANG tells waitpid not to block our thread. It just checks if the child is done.
            pid_t result = waitpid(current_bgsave, &status, WNOHANG);
            if (result == current_bgsave) {
                // Child finished successfully!
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
    char buffer[4096] = {0}; // Increased buffer size for larger RESP payloads
    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) break;
        buffer[bytes_read] = '\0';
        
        // Parse the RESP protocol into a vector of arguments
        std::vector<std::string> args = parse_resp(std::string(buffer));
        
        std::string response;
        if (args.empty()) {
            response = "-ERR Protocol error or unsupported command\r\n";
        } else {
            response = process_command(args);
        }
        
        write(client_fd, response.c_str(), response.length());
    }
    close(client_fd);
}

std::vector<std::string> Server::parse_resp(const std::string& input) {
    std::vector<std::string> args;
    // Basic RESP array validation: *<count>\r\n
    if (input.empty() || input[0] != '*') return args; 

    size_t pos = 1;
    size_t crlf = input.find("\r\n", pos);
    if (crlf == std::string::npos) return args;

    int num_args = std::stoi(input.substr(pos, crlf - pos));
    pos = crlf + 2;

    for (int i = 0; i < num_args; ++i) {
        // Look for bulk string prefix: $<length>\r\n
        if (pos >= input.length() || input[pos] != '$') break;
        pos++;
        
        crlf = input.find("\r\n", pos);
        if (crlf == std::string::npos) break;
        
        int len = std::stoi(input.substr(pos, crlf - pos));
        pos = crlf + 2;
        
        // Extract the actual string
        args.push_back(input.substr(pos, len));
        pos += len + 2; // Skip data and trailing \r\n
    }
    return args;
}

std::string Server::process_command(const std::vector<std::string>& args) {
    std::string command = args[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "PING") {
        return "+PONG\r\n"; // Simple String response
    }
    
    else if (command == "BGSAVE") {
        // 1. Check if a save is already happening
        if (bgsave_pid_.load() != -1) {
            return "-ERR Background save already in progress\r\n";
        }

        // 2. Lock the map strictly during the fork to ensure memory state is stable
        store_mutex_.lock(); 
        pid_t pid = fork();

        if (pid == 0) {
            // CHILD PROCESS: We are now in the clone
            std::ofstream rdb("dump.rdb", std::ios::trunc);
            
            for (const auto& [k, v] : store_) {
                rdb << k << " " << v.data << "\n";
            }
            
            rdb.flush();
            rdb.close();

            _exit(0); 

        } else if (pid > 0) {
            // PARENT PROCESS: Resume normal operations
            bgsave_pid_.store(pid);
            store_mutex_.unlock();
            return "+Background saving started\r\n";
        } else {
            store_mutex_.unlock();
            return "-ERR Failed to create background process\r\n";
        }
    }
    
    else if (command == "SET") {
        if (args.size() < 3) return "-ERR wrong number of arguments for 'set' command\r\n";
        std::string key = args[1];
        std::string value = args[2];

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        store_[key] = {value, {}, false};
        
        if (aof_stream_.is_open()) {
            aof_stream_ << "SET " << key << " " << value << "\n";
            aof_stream_.flush(); 
        }
        return "+OK\r\n"; // Simple String response

    } 
    else if (command == "EXPIRE") {
        if (args.size() != 3) return "-ERR wrong number of arguments for 'expire' command\r\n";
        std::string key = args[1];
        int seconds = std::stoi(args[2]);

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) {
            it->second.has_ttl = true;
            it->second.expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
            if (aof_stream_.is_open()) {
                aof_stream_ << "EXPIRE " << key << " " << seconds << "\n";
                aof_stream_.flush();
            }
            return ":1\r\n"; // Integer response (1 means success)
        }
        return ":0\r\n"; // Integer response (0 means key not found)

    } 
    else if (command == "GET") {
        if (args.size() != 2) return "-ERR wrong number of arguments for 'get' command\r\n";
        std::string key = args[1];
        bool expired = false;

        {
            std::shared_lock<std::shared_mutex> lock(store_mutex_);
            auto it = store_.find(key);
            if (it != store_.end()) {
                if (it->second.has_ttl && it->second.expires_at <= std::chrono::steady_clock::now()) {
                    expired = true; 
                } else {
                    // Bulk String response: $<length>\r\n<data>\r\n
                    return "$" + std::to_string(it->second.data.length()) + "\r\n" + it->second.data + "\r\n";
                }
            } else {
                return "$-1\r\n"; // Null Bulk String response
            }
        } 

        if (expired) {
            std::unique_lock<std::shared_mutex> lock(store_mutex_);
            store_.erase(key);
            if (aof_stream_.is_open()) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return "$-1\r\n"; // Null Bulk String response
        }

    } 
    else if (command == "DEL") {
        if (args.size() != 2) return "-ERR wrong number of arguments for 'del' command\r\n";
        std::string key = args[1];

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        if (store_.erase(key)) {
            if (aof_stream_.is_open()) {
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return ":1\r\n"; // Integer response (1 key deleted)
        }
        return ":0\r\n"; // Integer response (0 keys deleted)
    }
    
    return "-ERR unknown command '" + command + "'\r\n"; // Error response
}