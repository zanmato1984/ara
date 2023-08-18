cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_EXECUTOR_SRC "foo.cpp")
add_ara_library(ara-executor "${ARA_EXECUTOR_SRC}")

set(ARA_EXECUTOR_DEPS "ara-common")
add_ara_internal_dependencies(ara-executor "${ARA_EXECUTOR_DEPS}")
