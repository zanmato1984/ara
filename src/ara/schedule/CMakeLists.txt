cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_SCHEDULE_SRC "async_double_pool_scheduler.cpp" "naive_parallel_scheduler.cpp" "scheduler.cpp" "schedule_observer.cpp")
add_ara_library(ara-schedule "${ARA_SCHEDULE_SRC}")

set(ARA_SCHEDULE_DEPS "ara-common" "ara-executor" "ara-task" "ara-util")
add_ara_internal_dependencies(ara-schedule "${ARA_SCHEDULE_DEPS}")

set(ARA_SCHEDULE_TEST_SRC "schedule_test.cpp")
add_ara_test(ara-schedule-test "${ARA_SCHEDULE_TEST_SRC}")
