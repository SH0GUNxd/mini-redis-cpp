cmake_minimum_required(VERSION 3.15)
project(MiniRedis CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(mini-redis 
    src/main.cpp 
    src/server.cpp
)

target_link_libraries(mini-redis PRIVATE Threads::Threads)