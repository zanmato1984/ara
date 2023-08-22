#pragma once

#include <ara/common/defines.h>
#include <ara/common/query_context.h>
#include <ara/task/defines.h>

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
                                    const task::TaskResult&) {
    return Status::OK();
  }

  virtual Status OnTaskBackpressure(const ScheduleContext&, const task::Task&,
                                    task::TaskId) {
    return Status::OK();
  }
  virtual Status OnTaskBackpressureReset(const ScheduleContext&, const task::Task&,
                                         task::TaskId) {
    return Status::OK();
  }

  virtual Status OnTaskYield(const ScheduleContext&, const task::Task&, task::TaskId) {
    return Status::OK();
  }
  virtual Status OnTaskYieldBack(const ScheduleContext&, const task::Task&,
                                 task::TaskId) {
    return Status::OK();
  }

  virtual Status OnAllTasksFinished(const ScheduleContext&, const task::TaskGroup&,
                                    const std::vector<task::TaskResult>&) {
    return Status::OK();
  }

 public:
  static std::unique_ptr<ScheduleObserver> Make(const QueryContext&);
};

}  // namespace schedule

}  // namespace ara
