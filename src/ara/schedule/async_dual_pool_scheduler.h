#pragma once

#include <ara/schedule/scheduler.h>
#include <ara/task/defines.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>

#include <folly/Executor.h>
#include <folly/futures/Future.h>

namespace ara::schedule {

namespace detail {

using task::BackpressureAndResetPair;
using task::BackpressurePairFactory;
using task::Task;
using task::TaskContext;
using task::TaskGroup;
using task::TaskId;
using task::TaskResult;
using task::TaskStatus;

using Promise = folly::Promise<folly::Unit>;
using Future = folly::Future<task::TaskResult>;

class AsyncHandle : public TaskGroupHandle {
 private:
  static const std::string kName;

  using MakeFuture = std::function<Future(const TaskContext&, std::vector<TaskResult>&)>;

 public:
  AsyncHandle(const TaskGroup& task_group, TaskContext task_context,
              std::vector<TaskResult> results, MakeFuture make_future)
      : TaskGroupHandle(kName, task_group, std::move(task_context)),
        results_(std::move(results)),
        future_(make_future(task_context_, results_)) {}

 protected:
  TaskResult DoWait(const ScheduleContext&) override;

 private:
  std::vector<TaskResult> results_;
  Future future_;
};

class AsyncDualPoolScheduler : public Scheduler {
 public:
  static const std::string kName;
  static const std::string kDesc;

  AsyncDualPoolScheduler(folly::Executor* cpu_executor, folly::Executor* io_executor)
      : Scheduler(kName, kDesc), cpu_executor_(cpu_executor), io_executor_(io_executor) {}

 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const TaskGroup&) override;

 private:
  using ConcreteTask = std::pair<folly::Promise<folly::Unit>, folly::Future<TaskResult>>;

  ConcreteTask MakeTask(const ScheduleContext&, const Task&, const TaskContext&, TaskId,
                        TaskResult&) const;

  BackpressurePairFactory MakeBackpressurePairFactory(
      const ScheduleContext&) const override;

  Result<BackpressureAndResetPair> MakeBackpressureAndResetPair(const ScheduleContext&,
                                                                const TaskContext&,
                                                                const Task&,
                                                                TaskId) const;

 private:
  folly::Executor* cpu_executor_;
  folly::Executor* io_executor_;
};

}  // namespace detail

using AsyncHandle = detail::AsyncHandle;
using AsyncDualPoolScheduler = detail::AsyncDualPoolScheduler;

}  // namespace ara::schedule
