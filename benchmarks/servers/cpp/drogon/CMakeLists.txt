cmake_minimum_required(VERSION 3.14)
project(drogon-bench)

find_package(drogon REQUIRED)

add_executable(bench_server main.cc)
target_link_libraries(bench_server drogon)
