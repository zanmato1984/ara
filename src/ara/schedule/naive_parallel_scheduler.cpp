#include "naive_parallel_scheduler.h"

#include <ara/task/task_status.h>

namespace ara::schedule {

using task::TaskGroup;
using task::TaskResult;

TaskResult NaiveParallelHandle::DoWait(const ScheduleContext&) { return Status::OK(); }

Result<std::unique_ptr<TaskGroupHandle>> NaiveParallelScheduler::DoSchedule(
    const ScheduleContext&, const task::TaskGroup&) {
  return nullptr;
}

}  // namespace ara::schedule
