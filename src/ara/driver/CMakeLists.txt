cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

add_ara_library(ara-driver "foo.cpp")
add_ara_internal_dependencies(ara-driver ara-common)
