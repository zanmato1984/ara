#include "naive_parallel_scheduler.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>

namespace ara::schedule {

using task::Task;
using task::TaskContext;
using task::TaskGroup;
using task::TaskId;
using task::TaskResult;
using task::TaskStatus;

const std::string NaiveParallelHandle::kName = "NaiveParallelHandle";

TaskResult NaiveParallelHandle::DoWait(const ScheduleContext&) { return future_.get(); }

const std::string NaiveParallelScheduler::kName = "NaiveParallelScheduler";

Result<std::unique_ptr<TaskGroupHandle>> NaiveParallelScheduler::DoSchedule(
    const ScheduleContext& schedule_context, const TaskGroup& task_group) {
  auto& task = task_group.GetTask();
  auto num_tasks = task_group.NumTasks();
  auto& cont = task_group.GetContinuation();

  auto make_future = [&](const TaskContext& task_context) {
    std::vector<ConcreteTask> tasks;
    for (size_t i = 0; i < num_tasks; ++i) {
      tasks.push_back(MakeTask(schedule_context, task, task_context, i));
    }
    return std::async(std::launch::async,
                      [&, tasks = std::move(tasks)]() mutable -> TaskResult {
                        std::vector<TaskResult> results;
                        for (auto& task : tasks) {
                          results.push_back(task.get());
                        }
                        for (auto& result : results) {
                          ARA_RETURN_NOT_OK(result);
                        }
                        if (cont.has_value()) {
                          return cont.value()(task_context);
                        }
                        return TaskStatus::Finished();
                      });
  };
  auto task_context = MakeTaskContext(schedule_context);
  return std::make_unique<NaiveParallelHandle>(task_group.Name(), task_group.Desc(),
                                               std::move(task_context),
                                               std::move(make_future));
}

NaiveParallelScheduler::ConcreteTask NaiveParallelScheduler::MakeTask(
    const ScheduleContext&, const Task& task, const TaskContext& task_context,
    TaskId task_id) const {
  return std::async(std::launch::async, [&task, &task_context, task_id]() {
    TaskResult result = TaskStatus::Continue();
    while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
      auto result = task(task_context, task_id);
    }
    return result;
  });
}

}  // namespace ara::schedule
