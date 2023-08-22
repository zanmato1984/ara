#pragma once

#include <ara/schedule/scheduler.h>

namespace ara::schedule {

class NaiveParallelHandle : TaskGroupHandle {
 protected:
  task::TaskResult DoWait(const ScheduleContext&) override;
};

class NaiveParallelScheduler : Scheduler {
 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;
};

}  // namespace ara::schedule
