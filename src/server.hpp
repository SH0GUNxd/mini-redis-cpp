#pragma once
#include <string>
#include <unordered_map>

class Server {
public:
    Server(int port);
    ~Server();
    void start();
private:
    int port_;
    int server_fd_;
    std::unordered_map<std::string, std::string> store_;
    void handle_client(int client_fd);
    std::string process_command(const std::string& input);
};