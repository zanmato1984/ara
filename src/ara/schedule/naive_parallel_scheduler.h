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
  static const std::string kDesc;

 public:
  NaiveParallelScheduler() : Scheduler(kName, kDesc) {}

 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;

  task::ResumerFactory MakeResumerFactory(const ScheduleContext&) const override;
  task::SingleAwaiterFactory MakeSingleAwaiterFactgory(
      const ScheduleContext&) const override;
  task::AnyAwaiterFactory MakeAnyAwaiterFactgory(const ScheduleContext&) const override;
  task::AllAwaiterFactory MakeAllAwaiterFactgory(const ScheduleContext&) const override;

 private:
  using ConcreteTask = std::future<task::TaskResult>;

  ConcreteTask MakeTask(const ScheduleContext&, const task::Task&,
                        const task::TaskContext&, task::TaskId) const;
};

}  // namespace ara::schedule
