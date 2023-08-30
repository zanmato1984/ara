#include "naive_parallel_scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>

namespace ara::schedule {

using task::BackpressureAndResetPair;
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

namespace detail {
struct BackpressureImpl {
  std::mutex mutex;
  std::condition_variable cv;
  bool ready = false;
};
}  // namespace detail

std::optional<task::BackpressurePairFactory>
NaiveParallelScheduler::MakeBackpressurePairFactory(const ScheduleContext&) const {
  return
      [&](const TaskContext&, const Task&, TaskId) -> Result<BackpressureAndResetPair> {
        auto impl = std::make_shared<detail::BackpressureImpl>();
        auto callback = [impl]() {
          std::unique_lock<std::mutex> lock(impl->mutex);
          impl->ready = true;
          impl->cv.notify_one();
          return Status::OK();
        };
        return std::make_pair(std::any{std::move(impl)}, std::move(callback));
      };
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
          } else if (result->IsBackpressure()) {
            if (schedule_context.schedule_observer != nullptr) {
              ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskBackpressure, schedule_context, task,
                  task_id));
            }
            auto backpressure = std::any_cast<std::shared_ptr<detail::BackpressureImpl>>(
                std::move(result->GetBackpressure()));
            std::unique_lock<std::mutex> lock(backpressure->mutex);
            while (!backpressure->ready) {
              backpressure->cv.wait(lock);
            }
            if (schedule_context.schedule_observer != nullptr) {
              ARA_RETURN_NOT_OK(schedule_context.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskBackpressureReset, schedule_context, task,
                  task_id));
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
