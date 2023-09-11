#include "async_dual_pool_scheduler.h"
#include "naive_parallel_scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>

#include <arrow/testing/gtest_util.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;
using namespace ara::schedule;

struct AsyncDualPoolSchedulerHolder {
  folly::CPUThreadPoolExecutor cpu_executor{4};
  folly::IOThreadPoolExecutor io_executor{2};
  AsyncDualPoolScheduler scheduler{&cpu_executor, &io_executor};
};

struct NaiveParallelSchedulerHolder {
  NaiveParallelScheduler scheduler;
};

template <typename SchedulerType>
class ScheduleTest : public testing::Test {
 protected:
  TaskResult ScheduleTask(const ScheduleContext& schedule_context, Task task,
                          size_t num_tasks, std::optional<Continuation> cont,
                          TaskGroup::NotifyFinishFunc notify_finish) {
    TaskGroup task_group("", "", std::move(task), num_tasks, std::move(cont),
                         std::move(notify_finish));
    SchedulerType holder;
    auto handle = holder.scheduler.Schedule(schedule_context, task_group);
    ARA_RETURN_NOT_OK(handle);
    return (*handle)->Wait(schedule_context);
  }
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder>;
TYPED_TEST_SUITE(ScheduleTest, SchedulerTypes);

TYPED_TEST(ScheduleTest, EmptyTask) {
  ScheduleContext schedule_context;
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  auto result =
      this->ScheduleTask(schedule_context, std::move(task), 4, std::nullopt, nullptr);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TYPED_TEST(ScheduleTest, EmptyTaskWithEmptyCont) {
  ScheduleContext schedule_context;
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  Continuation cont("Cont", "Do nothing", [](const TaskContext&) -> TaskResult {
    return TaskStatus::Finished();
  });
  auto result =
      this->ScheduleTask(schedule_context, std::move(task), 4, std::move(cont), nullptr);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TYPED_TEST(ScheduleTest, EndlessTaskWithNotifyFinish) {
  ScheduleContext schedule_context;
  std::atomic_bool finished = false;
  Task task("Task", "Endless", [&](const TaskContext&, TaskId) -> TaskResult {
    return finished ? TaskStatus::Finished() : TaskStatus::Continue();
  });
  auto result = this->ScheduleTask(schedule_context, std::move(task), 4, std::nullopt,
                                   [&](const TaskContext&) {
                                     std::this_thread::sleep_for(std::chrono::seconds(1));
                                     finished = true;
                                     return Status::OK();
                                   });
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

// TODO: More schedule test: blocked/yield/error/cancel.
