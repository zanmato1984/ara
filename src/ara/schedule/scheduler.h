#pragma once

#include <ara/common/defines.h>

#include <memory>

namespace ara {

namespace task {
class TaskGroup;
}

namespace schedule {

class ScheduleContext;

class TaskGroupHandle {
 public:
  virtual ~TaskGroupHandle() = default;

  Status Wait(const ScheduleContext&);

 protected:
  virtual Status DoWait(const ScheduleContext&) = 0;
};

class Scheduler {
 public:
  virtual ~Scheduler() = default;

  Result<std::unique_ptr<TaskGroupHandle>> Schedule(const ScheduleContext&,
                                                    const task::TaskGroup&);

 protected:
  virtual Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                              const task::TaskGroup&) = 0;

 public:
  static std::unique_ptr<Scheduler> Make(const ScheduleContext&);
};

}  // namespace schedule

}  // namespace ara
