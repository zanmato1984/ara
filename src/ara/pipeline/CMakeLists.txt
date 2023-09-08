cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_PIPELINE_SRC
        "logical_pipeline.cpp"
        "pipeline_observer.cpp"
        "pipeline_task.cpp"
        "physical_pipeline.cpp")
add_ara_library(ara-pipeline "${ARA_PIPELINE_SRC}")

set(ARA_PIPELINE_DEPS
        "ara-common"
        "ara-task"
        "ara-util")
add_ara_internal_dependencies(ara-pipeline "${ARA_PIPELINE_DEPS}")

set(ARA_PIPELINE_COMPILE_TEST_SRC
        "pipeline_compile_test.cpp")
add_ara_test(ara-pipeline-compile-test ara-pipeline "${ARA_PIPELINE_COMPILE_TEST_SRC}")

set(ARA_PIPELINE_TASK_TEST_SRC
        "pipeline_task_test.cpp")
add_ara_test(ara-pipeline-task-test ara-pipeline "${ARA_PIPELINE_TASK_TEST_SRC}")
