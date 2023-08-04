cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

add_ara_library(ara-operator "foo.cpp")
add_ara_internal_dependencies(ara-operator ara-common)
