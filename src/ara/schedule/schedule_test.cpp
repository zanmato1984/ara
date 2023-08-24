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

TaskResult ScheduleTask(const ScheduleContext& schedule_context, Scheduler& scheduler,
                        Task task, size_t num_tasks, std::optional<Continuation> cont,
                        std::optional<TaskGroup::NotifyFinishFunc> notify_finish) {
  TaskGroup task_group("", "", std::move(task), num_tasks, std::move(cont),
                       std::move(notify_finish));
  auto handle = scheduler.Schedule(schedule_context, task_group);
  ARA_RETURN_NOT_OK(handle);
  return (*handle)->Wait(schedule_context);
}

TEST(ScheduleTest, AsyncDoublePoolSchedulerBasic) {
  ScheduleContext schedule_context;
  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(2);
  AsyncDoublePoolScheduler scheduler(&cpu_executor, &io_executor);
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  auto result = ScheduleTask(schedule_context, scheduler, std::move(task), 4,
                             std::nullopt, std::nullopt);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TEST(ScheduleTest, NaiveParallelSchedulerBasic) {
  ScheduleContext schedule_context;
  NaiveParallelScheduler scheduler;
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  auto result = ScheduleTask(schedule_context, scheduler, std::move(task), 4,
                             std::nullopt, std::nullopt);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}
