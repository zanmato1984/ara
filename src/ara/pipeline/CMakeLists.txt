cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_PIPELINE_SRC
        "logical_pipeline.cpp"
        "op/hash_aggregate.cpp"
        "op/hash_join.cpp"
        "op/hash_join_build.cpp"
        "op/hash_join_probe.cpp"
        "op/scalar_aggregate.cpp"
        "op/single_batch_parallel_source.cpp"
        "pipeline_observer.cpp"
        "pipeline_task.cpp"
        "physical_pipeline.cpp")
add_ara_library(ara-pipeline "${ARA_PIPELINE_SRC}")

set(ARA_PIPELINE_DEPS
        "ara-common"
        "ara-task"
        "ara-schedule"
        "ara-util")
add_ara_internal_dependencies(ara-pipeline "${ARA_PIPELINE_DEPS}")

set(ARA_PIPELINE_COMPILE_TEST_SRC
        "pipeline_compile_test.cpp")
add_ara_test(ara-pipeline-compile-test ara-pipeline "${ARA_PIPELINE_COMPILE_TEST_SRC}")

set(ARA_PIPELINE_TASK_TEST_SRC
        "pipeline_task_test.cpp")
add_ara_test(ara-pipeline-task-test ara-pipeline "${ARA_PIPELINE_TASK_TEST_SRC}")

set(ARA_HASH_JOIN_BUILD_TEST_SRC
        "op/hash_join_build_test.cpp")
add_ara_test(ara-hash-join-build-test ara-pipeline "${ARA_HASH_JOIN_BUILD_TEST_SRC}")

set(ARA_HASH_JOIN_PROBE_TEST_SRC
        "op/hash_join_probe_test.cpp")
add_ara_test(ara-hash-join-probe-test ara-pipeline "${ARA_HASH_JOIN_PROBE_TEST_SRC}")

set(ARA_SCALAR_AGGREGATE_TEST_SRC
        "op/scalar_aggregate_test.cpp")
add_ara_test(ara-scalar-aggregate-test ara-pipeline "${ARA_SCALAR_AGGREGATE_TEST_SRC}")

set(ARA_HASH_AGGREGATE_TEST_SRC
        "op/hash_aggregate_test.cpp")
add_ara_test(ara-hash-aggregate-test ara-pipeline "${ARA_HASH_AGGREGATE_TEST_SRC}")

set(ARA_SINGLE_BATCH_PARALLEL_SOURCE_TEST_SRC
        "op/single_batch_parallel_source_test.cpp")
add_ara_test(ara-single-batch-parallel-source-test ara-pipeline "${ARA_SINGLE_BATCH_PARALLEL_SOURCE_TEST_SRC}")
