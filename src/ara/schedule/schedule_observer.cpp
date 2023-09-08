#include "schedule_observer.h"
#include "scheduler.h"

#include <ara/task/task_group.h>
#include <ara/util/defines.h>
#include <ara/util/util.h>

namespace ara::schedule {

using task::Task;
using task::TaskGroup;
using task::TaskId;
using task::TaskResult;

class ScheduleLogger : public ScheduleObserver {
 public:
  Status OnScheduleTaskGroupBegin(const Scheduler&, const ScheduleContext&,
                                  const TaskGroup& task_group) override {
    ARA_LOG(INFO) << "Scheduling " << task_group.Name() << " begin";
    return Status::OK();
  }
  Status OnScheduleTaskGroupEnd(
      const Scheduler&, const ScheduleContext&, const TaskGroup& task_group,
      const Result<std::unique_ptr<TaskGroupHandle>>&) override {
    ARA_LOG(INFO) << "Scheduling " << task_group.Name() << " end";
    return Status::OK();
  }

  Status OnWaitTaskGroupBegin(const TaskGroupHandle& handle,
                              const ScheduleContext&) override {
    ARA_LOG(INFO) << "Waiting " << handle.Name() << " begin";
    return Status::OK();
  }
  Status OnWaitTaskGroupEnd(const TaskGroupHandle& handle, const ScheduleContext&,
                            const TaskResult&) override {
    ARA_LOG(INFO) << "Waiting " << handle.Name() << " end";
    return Status::OK();
  }

  Status OnTaskBlocked(const ScheduleContext&, const Task& task,
                       TaskId task_id) override {
    ARA_LOG(INFO) << "Task " << task.Name() << task_id << " blocked";
    return Status::OK();
  }
  Status OnTaskResumed(const ScheduleContext&, const Task& task,
                       TaskId task_id) override {
    ARA_LOG(INFO) << "Task " << task.Name() << task_id << " resumed";
    return Status::OK();
  }

  Status OnTaskYield(const ScheduleContext&, const Task& task, TaskId task_id) override {
    ARA_LOG(INFO) << "Task " << task.Name() << task_id << " yield";
    return Status::OK();
  }
  Status OnTaskYieldBack(const ScheduleContext&, const Task& task,
                         TaskId task_id) override {
    ARA_LOG(INFO) << "Task " << task.Name() << task_id << " yield back";
    return Status::OK();
  }

  Status OnAllTasksFinished(const ScheduleContext&, const TaskGroup& task_group,
                            const std::vector<TaskResult>&) override {
    ARA_LOG(INFO) << "All tasks of " << task_group.Name() << " finished";
    return Status::OK();
  }
};

std::unique_ptr<ChainedObserver<ScheduleObserver>> ScheduleObserver::Make(
    const QueryContext&) {
  auto logger = std::make_unique<ScheduleLogger>();
  std::vector<std::unique_ptr<ScheduleObserver>> observers;
  observers.push_back(std::move(logger));
  return std::make_unique<ChainedObserver<ScheduleObserver>>(std::move(observers));
}

}  // namespace ara::schedule
