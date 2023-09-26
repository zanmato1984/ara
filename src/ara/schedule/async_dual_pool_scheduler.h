#pragma once

#include <ara/schedule/scheduler.h>
#include <ara/task/defines.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>

#include <folly/Executor.h>
#include <folly/futures/Future.h>

namespace ara::schedule {

namespace detail {

using Promise = folly::Promise<folly::Unit>;
using Future = folly::Future<task::TaskResult>;

}  // namespace detail

class AsyncHandle : public TaskGroupHandle {
 private:
  static const std::string kName;

  using MakeFuture = std::function<detail::Future(const task::TaskContext&,
                                                  std::vector<task::TaskResult>&)>;

 public:
  AsyncHandle(const task::TaskGroup& task_group, task::TaskContext task_ctx,
              std::vector<task::TaskResult> results, MakeFuture make_future)
      : TaskGroupHandle(kName, task_group, std::move(task_ctx)),
        results_(std::move(results)),
        future_(make_future(task_ctx_, results_)) {}

 protected:
  task::TaskResult DoWait(const ScheduleContext&) override;

 private:
  std::vector<task::TaskResult> results_;
  detail::Future future_;
};

class AsyncDualPoolScheduler : public Scheduler {
 public:
  static const std::string kName;
  static const std::string kDesc;

  AsyncDualPoolScheduler(folly::Executor* cpu_executor, folly::Executor* io_executor)
      : Scheduler(kName, kDesc), cpu_executor_(cpu_executor), io_executor_(io_executor) {}

 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;

  task::ResumerFactory MakeResumerFactory(const ScheduleContext&) const override;
  task::SingleAwaiterFactory MakeSingleAwaiterFactgory(
      const ScheduleContext&) const override;
  task::AnyAwaiterFactory MakeAnyAwaiterFactgory(const ScheduleContext&) const override;
  task::AllAwaiterFactory MakeAllAwaiterFactgory(const ScheduleContext&) const override;

 private:
  using ConcreteTask =
      std::pair<folly::Promise<folly::Unit>, folly::Future<task::TaskResult>>;

  ConcreteTask MakeTask(const ScheduleContext&, const task::Task&,
                        const task::TaskContext&, task::TaskId, task::TaskResult&) const;

 private:
  folly::Executor* cpu_executor_;
  folly::Executor* io_executor_;
};

}  // namespace ara::schedule
