cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_COMMON_SRC
        "foo.cpp")
add_ara_library(ara-common "${ARA_COMMON_SRC}")

set(ARA_COMMON_DEPS
        "ara-util")
add_ara_internal_dependencies(ara-common "${ARA_COMMON_DEPS}")
