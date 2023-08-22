cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_TASK_SRC "task.cpp" "task_group.cpp" "task_observer.cpp")
add_ara_library(ara-task "${ARA_TASK_SRC}")

set(ARA_TASK_DEPS "ara-common" "ara-util")
add_ara_internal_dependencies(ara-task "${ARA_TASK_DEPS}")

set(ARA_TASK_TEST_SRC "task_test.cpp")
add_ara_test(ara-task-test "${ARA_TASK_TEST_SRC}")
