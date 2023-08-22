#pragma once

#include <ara/schedule/scheduler.h>
#include <ara/task/defines.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

namespace ara::schedule {

class FollyFutureHandle : public TaskGroupHandle {
 private:
  using Promise = folly::Promise<folly::Unit>;
  using Future = folly::Future<task::TaskResult>;

 public:
  FollyFutureHandle(Promise&& promise, Future&& future)
  {}
      // : promise_(std::move(promise)), future_(std::move(future)) {}

 protected:
  Status DoWait(const ScheduleContext&) override;

 private:
  // Promise promise_;
  // Future future_;
  std::vector<task::TaskResult> payload_;
};

class FollyFutureScheduler : public Scheduler {
 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;

 private:
  folly::CPUThreadPoolExecutor* cpu_executor;
  folly::IOThreadPoolExecutor* io_executor;
};

}  // namespace ara::schedule
