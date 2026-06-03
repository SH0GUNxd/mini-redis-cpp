#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <fstream>
#include "thread_pool.hpp"

class Server {
public:
    Server(int port);
    ~Server();

    void start();

private:
    int port_;
    int server_fd_;
    
    ThreadPool thread_pool_;
    std::unordered_map<std::string, std::string> store_;
    std::shared_mutex store_mutex_;

    std::ofstream aof_stream_;
    
    void load_aof();
    void handle_client(int client_fd);
    std::string process_command(const std::string& input);
};