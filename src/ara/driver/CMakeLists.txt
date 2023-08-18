cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_DRIVER_SRC "foo.cpp")
add_ara_library(ara-driver "${ARA_DRIVER_SRC}")

set(ARA_DRIVER_DEPS "ara-common")
add_ara_internal_dependencies(ara-driver "${ARA_DRIVER_DEPS}")
