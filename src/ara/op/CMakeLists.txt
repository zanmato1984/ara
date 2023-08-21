cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_OP_SRC "foo.cpp")
add_ara_library(ara-op "${ARA_OP_SRC}")

set(ARA_OP_DEPS "ara-common" "ara-task")
add_ara_internal_dependencies(ara-op "${ARA_OP_DEPS}")

set(ARA_OP_OUTPUT_TEST_SRC "op_output_test.cpp")
add_ara_test(ara-op-output-test "${ARA_OP_OUTPUT_TEST_SRC}")
