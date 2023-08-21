#include "folly_future_scheduler.h"

namespace ara::schedule {

Status FollyFutureHandle::DoWait(const ScheduleContext&) { return Status::OK(); }

Result<std::unique_ptr<TaskGroupHandle>> FollyFutureScheduler::DoSchedule(
    const ScheduleContext&, const task::TaskGroup&) {
  return std::make_unique<FollyFutureHandle>();
}

}  // namespace ara::schedule
