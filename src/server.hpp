#pragma once
#include <string>

class Server {
public:
    Server(int port);
    ~Server();
    void start();
private:
    int port_;
    int server_fd_;
    void handle_client(int client_fd);
};