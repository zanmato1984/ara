cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

add_ara_library(ara-util "foo.cpp")
target_link_libraries(ara-util PUBLIC ara-common)
