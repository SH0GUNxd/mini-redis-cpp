#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <fstream>
#include <chrono>
#include <thread>
#include <atomic>
#include "thread_pool.hpp"

struct CacheValue {
    std::string data;
    std::chrono::time_point<std::chrono::steady_clock> expires_at;
    bool has_ttl = false;
};

class Server {
public:
    Server(int port);
    ~Server();

    void start();

private:
    int port_;
    int server_fd_;
    
    ThreadPool thread_pool_;
    
    std::unordered_map<std::string, CacheValue> store_;
    std::shared_mutex store_mutex_;

    std::ofstream aof_stream_;
    
    std::thread eviction_thread_;
    std::atomic<bool> stop_eviction_{false};
    void eviction_loop();

    void load_aof();
    void handle_client(int client_fd);
    std::string process_command(const std::string& input);
};