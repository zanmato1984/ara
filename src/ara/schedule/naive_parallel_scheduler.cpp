#include "naive_parallel_scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"
#include "sync_awaiter.h"
#include "sync_resumer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>

namespace ara::schedule {

using task::AllAwaiterFactory;
using task::AnyAwaiterFactory;
using task::ResumerFactory;
using task::ResumerPtr;
using task::SingleAwaiterFactory;
using task::Task;
using task::TaskContext;
using task::TaskGroup;
using task::TaskId;
using task::TaskResult;
using task::TaskStatus;

const std::string NaiveParallelHandle::kName = "NaiveParallelHandle";

TaskResult NaiveParallelHandle::DoWait(const ScheduleContext&) { return future_.get(); }

const std::string NaiveParallelScheduler::kName = "NaiveParallelScheduler";
const std::string NaiveParallelScheduler::kDesc =
    "Scheduler that use naive std::thread for each task";

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
                        if (schedule_context.schedule_observer != nullptr) {
                          ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                              &ScheduleObserver::OnAllTasksFinished, schedule_context,
                              task_group, results));
                        }
                        if (cont.has_value()) {
                          return cont.value()(task_context);
                        }
                        return TaskStatus::Finished();
                      });
  };
  auto task_context = MakeTaskContext(schedule_context);
  return std::make_unique<NaiveParallelHandle>(task_group, std::move(task_context),
                                               std::move(make_future));
}

ResumerFactory NaiveParallelScheduler::MakeResumerFactory(const ScheduleContext&) const {
  return []() -> Result<ResumerPtr> { return std::make_shared<SyncResumer>(); };
}

SingleAwaiterFactory NaiveParallelScheduler::MakeSingleAwaiterFactgory(
    const ScheduleContext&) const {
  return SyncAwaiter::MakeSingle;
}

AnyAwaiterFactory NaiveParallelScheduler::MakeAnyAwaiterFactgory(
    const ScheduleContext&) const {
  return SyncAwaiter::MakeAny;
}

AllAwaiterFactory NaiveParallelScheduler::MakeAllAwaiterFactgory(
    const ScheduleContext&) const {
  return SyncAwaiter::MakeAll;
}

NaiveParallelScheduler::ConcreteTask NaiveParallelScheduler::MakeTask(
    const ScheduleContext& schedule_context, const Task& task,
    const TaskContext& task_context, TaskId task_id) const {
  return std::async(
      std::launch::async,
      [&schedule_context, &task, &task_context, task_id]() -> TaskResult {
        TaskResult result = TaskStatus::Continue();
        while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
          bool is_yield = false;
          if (result->IsYield()) {
            is_yield = true;
            if (schedule_context.schedule_observer != nullptr) {
              ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskYield, schedule_context, task, task_id));
            }
          } else if (result->IsBlocked()) {
            if (schedule_context.schedule_observer != nullptr) {
              ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskBlocked, schedule_context, task, task_id));
            }
            auto awaiter = std::dynamic_pointer_cast<SyncAwaiter>(result->GetAwaiter());
            ARA_CHECK(awaiter != nullptr);
            awaiter->Wait();
            if (schedule_context.schedule_observer != nullptr) {
              ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskResumed, schedule_context, task, task_id));
            }
          }
          result = task(task_context, task_id);
          if (is_yield && result.ok() && !result->IsYield()) {
            if (schedule_context.schedule_observer != nullptr) {
              ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskYieldBack, schedule_context, task, task_id));
            }
          }
        }
        return result;
      });
}

}  // namespace ara::schedule
