cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

add_ara_library(ara-op "foo.cpp")
add_ara_internal_dependencies(ara-op ara-common)

add_ara_test(ara-op-output-test "op_output_test.cpp")
