#pragma once

#include <ara/schedule/scheduler.h>
#include <ara/task/backpressure.h>
#include <ara/task/defines.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

namespace ara::schedule {

namespace detail {

using task::BackpressureAndResetPair;
using task::Task;
using task::TaskContext;
using task::TaskGroup;
using task::TaskId;
using task::TaskObserver;
using task::TaskResult;
using task::TaskStatus;

using Promise = folly::Promise<folly::Unit>;
using Future = folly::Future<task::TaskResult>;

class FollyFutureHandle : public TaskGroupHandle {
 private:
  using MakeFuture = std::function<Future(const TaskContext&, std::vector<TaskResult>&)>;

 public:
  static const std::string kName;

  FollyFutureHandle(std::string name, std::string desc, TaskContext task_context,
                    std::vector<TaskResult> results, MakeFuture make_future)
      : TaskGroupHandle(kName + "(" + std::move(name) + ")",
                        kName + ")" + std::move(desc) + ")"),
        task_context_(std::move(task_context)),
        results_(std::move(results)),
        future_(make_future(task_context_, results_)) {}

 protected:
  TaskResult DoWait(const ScheduleContext&) override;

 private:
  TaskContext task_context_;
  std::vector<TaskResult> results_;
  Future future_;
};

class FollyFutureScheduler : public Scheduler {
 public:
  static const std::string kName;

  FollyFutureScheduler(folly::CPUThreadPoolExecutor* cpu_executor,
                       folly::IOThreadPoolExecutor* io_executor)
      : Scheduler(kName), cpu_executor_(cpu_executor), io_executor_(io_executor) {}

 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const TaskGroup&) override;

 private:
  TaskContext MakeTaskContext(const ScheduleContext&) const;

  using ConcreteTask = std::pair<folly::Promise<folly::Unit>, folly::Future<TaskResult>>;

  ConcreteTask MakeTask(const ScheduleContext&, const Task&, const TaskContext&, TaskId,
                        TaskResult&) const;

  Result<BackpressureAndResetPair> MakeBackpressureAndResetPair(const ScheduleContext&,
                                                                const TaskContext&,
                                                                const Task&,
                                                                TaskId) const;

 private:
  folly::CPUThreadPoolExecutor* cpu_executor_;
  folly::IOThreadPoolExecutor* io_executor_;
};

}  // namespace detail

using FollyFutureHandle = detail::FollyFutureHandle;
using FollyFutureScheduler = detail::FollyFutureScheduler;

}  // namespace ara::schedule
