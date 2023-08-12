cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(SKETCH_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_test.cpp")
add_ara_test(sketch_test "${SKETCH_TEST_SRC}")

set(SKETCH_ARROW_HASH_JOIN_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_arrow_hash_join_test.cpp")
add_ara_test(sketch_arrow_hash_join_test "${SKETCH_ARROW_HASH_JOIN_TEST_SRC}")

set(SKETCH_ARROW_SWISS_JOIN_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_arrow_swiss_join_test.cpp")
add_ara_test(sketch_arrow_swiss_join_test "${SKETCH_ARROW_SWISS_JOIN_TEST_SRC}")

set(SKETCH_OPERATOR_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_operator_test.cpp")
add_ara_test(sketch_operator_test "${SKETCH_OPERATOR_TEST_SRC}")

set(SKETCH_PIPELINE_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_pipeline_test.cpp")
add_ara_test(sketch_pipeline_test "${SKETCH_PIPELINE_TEST_SRC}")

set(SKETCH_TASK_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_task_test.cpp")
add_ara_test(sketch_task_test "${SKETCH_TASK_TEST_SRC}")

set(SKETCH_FUTURE_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_future_test.cpp")
add_ara_test(sketch_future_test "${SKETCH_FUTURE_TEST_SRC}")

set(SKETCH_HASH_JOIN_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_hash_join.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_hash_join_test.cpp")
add_ara_test(sketch_hash_join_test "${SKETCH_HASH_JOIN_TEST_SRC}")

set(SKETCH_RESULT_AND_STATUS_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_result_and_status_test.cpp")
add_ara_test(sketch_result_and_status_test "${SKETCH_RESULT_AND_STATUS_TEST_SRC}")

set(SKETCH_CORO_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_coro_test.cpp")
add_ara_test(sketch_coro_test "${SKETCH_CORO_TEST_SRC}")

set(SKETCH_AGGREGATION_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_aggregation_test.cpp")
add_ara_test(sketch_aggregation_test "${SKETCH_AGGREGATION_TEST_SRC}")

set(SKETCH_ASYNC_BACKPRESSURE_TEST_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/sketch_async_backpressure_test.cpp")
add_ara_test(sketch_async_backpressure_test "${SKETCH_ASYNC_BACKPRESSURE_TEST_SRC}")
