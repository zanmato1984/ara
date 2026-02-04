cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(ARA_SCHEDULE_SRC
        "async_awaiter.cpp"
        "async_dual_pool_scheduler.cpp"
        "coro_awaiter.cpp"
        "naive_parallel_scheduler.cpp"
        "scheduler.cpp"
        "schedule_observer.cpp"
        "sequential_coro_scheduler.cpp"
        "sync_awaiter.cpp")
add_ara_library(ara-schedule "${ARA_SCHEDULE_SRC}")

set(ARA_SCHEDULE_DEPS
        "ara-common"
        "ara-executor"
        "ara-task"
        "ara-util")
add_ara_internal_dependencies(ara-schedule "${ARA_SCHEDULE_DEPS}")

set(ARA_ASYNC_RESUMER_AWAITER_TEST_SRC
        "async_resumer_awaiter_test.cpp")
add_ara_test(ara-async-resumer-awaiter-test ara-schedule "${ARA_ASYNC_RESUMER_AWAITER_TEST_SRC}")

set(ARA_SYNC_RESUMER_AWAITER_TEST_SRC
        "sync_resumer_awaiter_test.cpp")
add_ara_test(ara-sync-resumer-awaiter-test ara-schedule "${ARA_SYNC_RESUMER_AWAITER_TEST_SRC}")

set(ARA_CORO_RESUMER_AWAITER_TEST_SRC
        "coro_resumer_awaiter_test.cpp")
add_ara_test(ara-coro-resumer-awaiter-test ara-schedule "${ARA_CORO_RESUMER_AWAITER_TEST_SRC}")

set(ARA_SCHEDULER_TEST_SRC
        "scheduler_test.cpp")
add_ara_test(ara-scheduler-test ara-schedule "${ARA_SCHEDULER_TEST_SRC}")
