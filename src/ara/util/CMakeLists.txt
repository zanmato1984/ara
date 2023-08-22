cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_UTIL_SRC "util.cpp")
add_ara_library(ara-util "${ARA_UTIL_SRC}")

set(ARA_UTIL_DEPS "ara-common")
add_ara_internal_dependencies(ara-util "${ARA_UTIL_DEPS}")
