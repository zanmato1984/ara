#pragma once

#include <ara/schedule/scheduler.h>

namespace ara::schedule {

class FollyFutureHandle : public TaskGroupHandle {
 protected:
  Status DoWait(const ScheduleContext&) override;
};

class FollyFutureScheduler : public Scheduler {
 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;
};

}  // namespace ara::schedule
