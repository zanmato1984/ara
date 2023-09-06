#pragma once

#include <ara/common/defines.h>
#include <ara/common/meta.h>
#include <ara/common/query_context.h>
#include <ara/task/backpressure.h>
#include <ara/task/defines.h>
#include <ara/task/task_context.h>

#include <memory>

namespace ara {

namespace task {
class TaskGroup;
}

namespace schedule {

class ScheduleContext;

class TaskGroupHandle : public internal::Meta {
 public:
  TaskGroupHandle(const std::string&, const task::TaskGroup&, task::TaskContext);

  virtual ~TaskGroupHandle() = default;

  task::TaskResult Wait(const ScheduleContext&);

 protected:
  virtual task::TaskResult DoWait(const ScheduleContext&) = 0;

 protected:
  const task::TaskGroup& task_group_;
  task::TaskContext task_context_;
};

class Scheduler : public internal::Meta {
 public:
  Scheduler(std::string name, std::string desc)
      : Meta(std::move(name), std::move(desc)) {}

  virtual ~Scheduler() = default;

  const std::string& Name() const { return name_; }

  Result<std::unique_ptr<TaskGroupHandle>> Schedule(const ScheduleContext&,
                                                    const task::TaskGroup&);

 protected:
  virtual Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                              const task::TaskGroup&) = 0;

  task::TaskContext MakeTaskContext(const ScheduleContext&) const;

  virtual task::BackpressurePairFactory MakeBackpressurePairFactory(
      const ScheduleContext&) const = 0;

 private:
  std::string name_;

 public:
  static std::unique_ptr<Scheduler> Make(const QueryContext&);
};

}  // namespace schedule

}  // namespace ara
