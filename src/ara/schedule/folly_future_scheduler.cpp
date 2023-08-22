#include "folly_future_scheduler.h"

namespace ara::schedule {

using task::TaskResult;
using task::TaskStatus;

Status FollyFutureHandle::DoWait(const ScheduleContext&) { return Status::OK(); }

Result<std::unique_ptr<TaskGroupHandle>> FollyFutureScheduler::DoSchedule(
    const ScheduleContext&, const task::TaskGroup&) {
  auto [promise, future] = folly::makePromiseContract<folly::Unit>();
  auto future2 = std::move(future).via(cpu_executor).thenValue([](auto&&) -> TaskResult {
    return TaskStatus::Continue();
  });
  return std::make_unique<FollyFutureHandle>(std::move(promise), std::move(future2));
}

}  // namespace ara::schedule
