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

constexpr size_t cpu_thread_pool_size = 4;
constexpr size_t io_thread_pool_size = 2;

struct AsyncDualPoolSchedulerHolder {
  folly::CPUThreadPoolExecutor cpu_executor{cpu_thread_pool_size};
  folly::IOThreadPoolExecutor io_executor{io_thread_pool_size};
  AsyncDualPoolScheduler scheduler{&cpu_executor, &io_executor};
};

struct NaiveParallelSchedulerHolder {
  NaiveParallelScheduler scheduler;
};

template <typename SchedulerType>
class ScheduleTest : public testing::Test {
 protected:
  TaskResult ScheduleTask(const ScheduleContext& schedule_ctx, Task task,
                          size_t num_tasks, std::optional<Continuation> cont,
                          TaskGroup::NotifyFinishFunc notify_finish) {
    TaskGroup task_group("", "", std::move(task), num_tasks, std::move(cont),
                         std::move(notify_finish));
    SchedulerType holder;
    auto handle = holder.scheduler.Schedule(schedule_ctx, task_group);
    ARA_RETURN_NOT_OK(handle);
    return (*handle)->Wait(schedule_ctx);
  }
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder>;
TYPED_TEST_SUITE(ScheduleTest, SchedulerTypes);

TYPED_TEST(ScheduleTest, EmptyTask) {
  ScheduleContext schedule_ctx;
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  auto result =
      this->ScheduleTask(schedule_ctx, std::move(task), 4, std::nullopt, nullptr);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TYPED_TEST(ScheduleTest, EmptyTaskWithEmptyCont) {
  ScheduleContext schedule_ctx;
  Task task("Task", "Do nothing", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });
  Continuation cont("Cont", "Do nothing", [](const TaskContext&) -> TaskResult {
    return TaskStatus::Finished();
  });
  auto result =
      this->ScheduleTask(schedule_ctx, std::move(task), 4, std::move(cont), nullptr);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TYPED_TEST(ScheduleTest, ContAfterTask) {
  ScheduleContext schedule_ctx;
  std::atomic<size_t> counter = 0, cont_saw = 0;
  size_t num_tasks = 42;
  Task task("Task", "Do nothing", [&](const TaskContext&, TaskId) -> TaskResult {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    counter++;
    return TaskStatus::Finished();
  });
  Continuation cont("Cont", "Do nothing", [&](const TaskContext&) -> TaskResult {
    cont_saw = counter.load();
    return TaskStatus::Finished();
  });
  auto result = this->ScheduleTask(schedule_ctx, std::move(task), num_tasks,
                                   std::move(cont), nullptr);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(cont_saw, num_tasks);
}

TYPED_TEST(ScheduleTest, EndlessTaskWithNotifyFinish) {
  ScheduleContext schedule_ctx;
  std::atomic_bool finished = false;
  Task task("Task", "Endless", [&](const TaskContext&, TaskId) -> TaskResult {
    return finished ? TaskStatus::Finished() : TaskStatus::Continue();
  });
  auto result = this->ScheduleTask(schedule_ctx, std::move(task), 4, std::nullopt,
                                   [&](const TaskContext&) {
                                     std::this_thread::sleep_for(std::chrono::seconds(1));
                                     finished = true;
                                     return Status::OK();
                                   });
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}

TYPED_TEST(ScheduleTest, YieldTask) {
  ScheduleContext schedule_ctx;
  size_t num_tasks = cpu_thread_pool_size * 4;
  std::mutex cpu_thread_ids_mutex, io_thread_ids_mutex;
  std::unordered_set<std::thread::id> cpu_thread_ids, io_thread_ids;
  std::vector<bool> task_yielded(num_tasks, false);

  Task task("YieldTask", "", [&](const TaskContext&, TaskId task_id) -> TaskResult {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (!task_yielded[task_id]) {
      task_yielded[task_id] = true;
      {
        std::lock_guard<std::mutex> lock(cpu_thread_ids_mutex);
        cpu_thread_ids.insert(std::this_thread::get_id());
      }
      return TaskStatus::Yield();
    }
    {
      std::lock_guard<std::mutex> lock(io_thread_ids_mutex);
      io_thread_ids.insert(std::this_thread::get_id());
    }
    return TaskStatus::Finished();
  });

  auto result =
      this->ScheduleTask(schedule_ctx, std::move(task), num_tasks, std::nullopt, nullptr);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());

  // TODO: Ensure that the yield tasks are ran in IO thread pool.
  // for (auto io_thread_id : io_thread_ids) {
  //   ASSERT_TRUE(cpu_thread_ids.find(io_thread_id) == cpu_thread_ids.end());
  // }
}

TYPED_TEST(ScheduleTest, BlockedTask) {
  ScheduleContext schedule_ctx;
  size_t num_tasks = 42;
  std::atomic<size_t> counter = 0;
  Resumers resumers(num_tasks);
  std::atomic<size_t> num_resumers_set = 0;

  Task blocked_task(
      "BlockedTask", "", [&](const TaskContext& task_ctx, TaskId task_id) -> TaskResult {
        if (resumers[task_id] == nullptr) {
          ARA_CHECK(task_ctx.resumer_factory != nullptr);
          ARA_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
          ARA_CHECK(task_ctx.single_awaiter_factory != nullptr);
          ARA_ASSIGN_OR_RAISE(auto awaiter, task_ctx.single_awaiter_factory(resumer));
          resumers[task_id] = std::move(resumer);
          num_resumers_set++;
          return TaskStatus::Blocked(std::move(awaiter));
        }
        counter++;
        return TaskStatus::Finished();
      });
  Task resumer_task("ResumerTask", "",
                    [&](const TaskContext&, TaskId) {
                      if (num_resumers_set != num_tasks) {
                        return TaskStatus::Continue();
                      }
                      std::this_thread::sleep_for(std::chrono::seconds(1));
                      for (auto& resumer : resumers) {
                        ARA_CHECK(resumer != nullptr);
                        resumer->Resume();
                      }
                      return TaskStatus::Finished();
                    },
                    {TaskHint::Type::IO});

  auto blocked_task_future = std::async(std::launch::async, [&]() -> TaskResult {
    return this->ScheduleTask(schedule_ctx, std::move(blocked_task), num_tasks,
                              std::nullopt, nullptr);
  });
  auto resumer_task_future = std::async(std::launch::async, [&]() -> TaskResult {
    return this->ScheduleTask(schedule_ctx, std::move(resumer_task), 1, std::nullopt,
                              nullptr);
  });

  {
    auto result = blocked_task_future.get();
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(counter, num_tasks);
  }
  {
    auto result = resumer_task_future.get();
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(counter, num_tasks);
  }
}

TYPED_TEST(ScheduleTest, ErrorAndCancel) {
  ScheduleContext schedule_ctx;
  size_t num_errors = 4, num_tasks = 42;
  std::atomic<int> error_counts = -1;
  Task task("Task", "Error then cancel", [&](const TaskContext&, TaskId) -> TaskResult {
    if (error_counts < 0) {
      return TaskStatus::Continue();
    }
    if (error_counts++ < num_errors) {
      return Status::UnknownError("42");
    }
    return TaskStatus::Cancelled();
  });
  auto result = this->ScheduleTask(schedule_ctx, std::move(task), num_tasks, std::nullopt,
                                   [&](const TaskContext&) {
                                     std::this_thread::sleep_for(std::chrono::seconds(1));
                                     error_counts = 0;
                                     return Status::OK();
                                   });
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.status().message(), "42");
}
