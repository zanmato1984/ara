cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(SKETCH_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_test.cpp")
configure_test(sketch_test "${SKETCH_TEST_SRC}")

set(SKETCH_ARROW_HASH_JOIN_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_arrow_hash_join_test.cpp")
configure_test(sketch_arrow_hash_join_test "${SKETCH_ARROW_HASH_JOIN_TEST_SRC}")

set(SKETCH_ARROW_SWISS_JOIN_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_arrow_swiss_join_test.cpp")
configure_test(sketch_arrow_swiss_join_test "${SKETCH_ARROW_SWISS_JOIN_TEST_SRC}")

set(SKETCH_OPERATOR_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_operator_test.cpp")
configure_test(sketch_operator_test "${SKETCH_OPERATOR_TEST_SRC}")

set(SKETCH_PIPELINE_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_pipeline_test.cpp")
configure_test(sketch_pipeline_test "${SKETCH_PIPELINE_TEST_SRC}")

set(SKETCH_TASK_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_task_test.cpp")
configure_test(sketch_task_test "${SKETCH_TASK_TEST_SRC}")

set(SKETCH_FUTURE_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_future_test.cpp")
configure_test(sketch_future_test "${SKETCH_FUTURE_TEST_SRC}")

set(SKETCH_HASH_JOIN_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_hash_join.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_hash_join_test.cpp")
configure_test(sketch_hash_join_test "${SKETCH_HASH_JOIN_TEST_SRC}")
