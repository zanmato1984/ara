cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_PLAN_SRC
        "foo.cpp")
add_ara_library(ara-plan "${ARA_PLAN_SRC}")

set(ARA_PLAN_DEPS
        "ara-common"
        "ara-util")
add_ara_internal_dependencies(ara-plan "${ARA_PLAN_DEPS}")
