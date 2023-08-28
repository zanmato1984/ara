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

set(ARA_PIPELINE_TEST_SRC
        "pipeline_test.cpp")
add_ara_test(ara-pipeline-test "${ARA_PIPELINE_TEST_SRC}")
