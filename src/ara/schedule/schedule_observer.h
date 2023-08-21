#pragma once

#include <ara/common/defines.h>

namespace ara {

namespace task {
class TaskGroup;
}  // namespace task

namespace schedule {

class ScheduleContext;
class TaskGroupHandle;
class Scheduler;

class ScheduleObserver {
 public:
  virtual ~ScheduleObserver() = default;

  virtual Status OnScheduleTaskGroupBegin(const Scheduler&, const ScheduleContext&,
                                          const task::TaskGroup&) {
    return Status::OK();
  }
  virtual Status OnScheduleTaskGroupEnd(const Scheduler&, const ScheduleContext&,
                                        const task::TaskGroup&,
                                        const Result<std::unique_ptr<TaskGroupHandle>>&) {
    return Status::OK();
  }

  virtual Status OnWaitTaskGroupBegin(const TaskGroupHandle&, const ScheduleContext&) {
    return Status::OK();
  }
  virtual Status OnWaitTaskGroupEnd(const TaskGroupHandle&, const ScheduleContext&,
                                    const Status&) {
    return Status::OK();
  }
};

}  // namespace schedule

}  // namespace ara
