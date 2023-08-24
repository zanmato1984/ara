#pragma once

#include <ara/schedule/scheduler.h>
#include <ara/task/task_context.h>

#include <future>

namespace ara::schedule {

class NaiveParallelHandle : public TaskGroupHandle {
 private:
  static const std::string kName;

  using MakeFuture =
      std::function<std::future<task::TaskResult>(const task::TaskContext&)>;

 public:
  NaiveParallelHandle(const task::TaskGroup& task_group, task::TaskContext task_context,
                      MakeFuture make_future)
      : TaskGroupHandle(kName, task_group, std::move(task_context)),
        future_(make_future(task_context_)) {}

 protected:
  task::TaskResult DoWait(const ScheduleContext&) override;

 private:
  std::future<task::TaskResult> future_;
};

class NaiveParallelScheduler : public Scheduler {
 private:
  static const std::string kName;

 public:
  NaiveParallelScheduler() : Scheduler(kName) {}

 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;

  std::optional<task::BackpressurePairFactory> MakeBackpressurePairFactory(
      const ScheduleContext&) const override;

 private:
  using ConcreteTask = std::future<task::TaskResult>;

  ConcreteTask MakeTask(const ScheduleContext&, const task::Task&,
                        const task::TaskContext&, task::TaskId) const;
};

}  // namespace ara::schedule
