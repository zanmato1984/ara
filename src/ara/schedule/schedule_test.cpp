#include "async_double_pool_scheduler.h"
#include "naive_parallel_scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>

#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;
using namespace ara::schedule;

TEST(ScheduleTest, AsyncDoublePoolSchedulerBasic) {
  ScheduleContext schedule_context;
  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(2);
  AsyncDoublePoolScheduler scheduler(&cpu_executor, &io_executor);
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  TaskGroup task_group("TaskGroup", "Do nothing", std::move(task), 4, std::nullopt,
                       std::nullopt);

  auto handle = scheduler.Schedule(schedule_context, task_group);
  ASSERT_OK(handle);
  auto result = (*handle)->Wait(schedule_context);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TEST(ScheduleTest, NaiveParallelSchedulerBasic) {
  ScheduleContext schedule_context;
  NaiveParallelScheduler scheduler;
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  TaskGroup task_group("TaskGroup", "Do nothing", std::move(task), 4, std::nullopt,
                       std::nullopt);

  auto handle = scheduler.Schedule(schedule_context, task_group);
  ASSERT_OK(handle);
  auto result = (*handle)->Wait(schedule_context);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}
