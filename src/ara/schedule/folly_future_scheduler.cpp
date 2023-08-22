#include "folly_future_scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/util/util.h>

namespace ara::schedule::detail {

const std::string FollyFutureHandle::kName = "FollyFutureHandle";

TaskResult FollyFutureHandle::DoWait(const ScheduleContext&) {
  return future_.wait().value();
}

const std::string FollyFutureScheduler::kName = "FollyFutureScheduler";

Result<std::unique_ptr<TaskGroupHandle>> FollyFutureScheduler::DoSchedule(
    const ScheduleContext& schedule_context, const TaskGroup& task_group) {
  auto& task = task_group.GetTask();
  auto num_tasks = task_group.NumTasks();
  auto& cont = task_group.GetContinuation();

  auto make_future = [&](const TaskContext& task_context,
                         std::vector<TaskResult>& results) -> Future {
    std::vector<Promise> task_promises;
    std::vector<Future> task_futures;
    for (size_t i = 0; i < num_tasks; ++i) {
      auto [tp, tf] = MakeTask(schedule_context, task, task_context, i, results[i]);
      task_promises.push_back(std::move(tp));
      task_futures.push_back(std::move(tf));
      results[i] = TaskStatus::Continue();
    }
    auto [p, temp_f] = folly::makePromiseContract<folly::Unit>(cpu_executor_);
    auto f = std::move(temp_f)
                 .thenValue([task_promises = std::move(task_promises),
                             task_futures = std::move(task_futures)](auto&&) mutable {
                   for (size_t i = 0; i < task_promises.size(); ++i) {
                     task_promises[i].setValue();
                   }
                   return folly::collectAll(task_futures);
                 })
                 .thenValue([&schedule_context, &task_group, &cont,
                             &task_context](auto&& try_results) -> TaskResult {
                   std::vector<TaskResult> results(try_results.size());
                   std::transform(try_results.begin(), try_results.end(), results.begin(),
                                  [](auto&& try_result) -> TaskResult {
                                    ARA_CHECK(try_result.hasValue());
                                    return try_result.value();
                                  });
                   if (schedule_context.schedule_observer != nullptr) {
                     auto status = schedule_context.schedule_observer->OnAllTasksFinished(
                         schedule_context, task_group, results);
                     if (!status.ok()) {
                       return std::move(status);
                     }
                   }
                   for (auto&& result : results) {
                     ARA_RETURN_NOT_OK(result);
                   }
                   if (cont.has_value()) {
                     return cont.value()(task_context);
                   }
                   return TaskStatus::Finished();
                 });
    p.setValue();
    return std::move(f);
  };
  TaskContext task_context = MakeTaskContext(schedule_context);
  std::vector<TaskResult> results(num_tasks);

  return std::make_unique<FollyFutureHandle>(task_group.Name(), task_group.Desc(),
                                             std::move(task_context), std::move(results),
                                             std::move(make_future));
}

TaskContext FollyFutureScheduler::MakeTaskContext(
    const ScheduleContext& schedule_context) const {
  auto task_observer = TaskObserver::Make(*schedule_context.query_context);
  return {schedule_context.query_context, schedule_context.query_id,
          [&](const TaskContext& task_context, const Task& task,
              TaskId task_id) -> Result<BackpressureAndResetPair> {
            return MakeBackpressureAndResetPair(schedule_context, task_context, task,
                                                task_id);
          },
          std::move(task_observer)};
}

FollyFutureScheduler::ConcreteTask FollyFutureScheduler::MakeTask(
    const ScheduleContext& schedule_context, const Task& task, const TaskContext& context,
    TaskId task_id, TaskResult& result) const {
  auto [p, f] = folly::makePromiseContract<folly::Unit>(cpu_executor_);
  auto pred = [&]() {
    return result.ok() && !result->IsFinished() && !result->IsCancelled();
  };
  auto thunk = [&, task_id]() {
    if (result->IsBackpressure()) {
      if (schedule_context.schedule_observer != nullptr) {
        auto status = schedule_context.schedule_observer->OnTaskBackpressure(
            schedule_context, task, task_id);
        if (!status.ok()) {
          result = std::move(status);
          return folly::makeFuture();
        }
      }
      auto backpressure = std::any_cast<std::shared_ptr<folly::Future<folly::Unit>>>(
          std::move(result->GetBackpressure()));
      return std::move(*backpressure).thenValue([&, task_id](auto&&) {
        if (schedule_context.schedule_observer != nullptr) {
          auto status = schedule_context.schedule_observer->OnTaskBackpressureReset(
              schedule_context, task, task_id);
          if (!status.ok()) {
            result = std::move(status);
            return;
          }
        }
        result = TaskStatus::Continue();
      });
    }

    if (result->IsYield()) {
      if (schedule_context.schedule_observer != nullptr) {
        auto status = schedule_context.schedule_observer->OnTaskYield(schedule_context,
                                                                      task, task_id);
        if (!status.ok()) {
          result = std::move(status);
          return folly::makeFuture();
        }
      }
      return folly::via(io_executor_).then([&, task_id](auto&&) {
        result = task(context, task_id);
        if (!result.ok()) {
          return;
        }
        if (schedule_context.schedule_observer != nullptr) {
          auto status = schedule_context.schedule_observer->OnTaskYieldBack(
              schedule_context, task, task_id);
          if (!status.ok()) {
            result = std::move(status);
          }
        }
      });
    }

    return folly::via(cpu_executor_).then([&, task_id](auto&&) {
      result = task(context, task_id);
    });
  };

  auto task_f =
      std::move(f).thenValue([pred, thunk, &result](auto&&) -> folly::Future<TaskResult> {
        return folly::whileDo(pred, thunk).thenValue([&](auto&&) {
          return std::move(result);
        });
      });

  return {std::move(p), std::move(task_f)};
}

Result<BackpressureAndResetPair> FollyFutureScheduler::MakeBackpressureAndResetPair(
    const ScheduleContext& schedule_context, const TaskContext& task_context,
    const Task& task, TaskId task_id) const {
  auto [p, f] = folly::makePromiseContract<folly::Unit>(cpu_executor_);
  // Workaround that std::function must be copy-constructible.
  auto p_ptr = std::make_shared<folly::Promise<folly::Unit>>(std::move(p));
  auto f_ptr = std::make_shared<folly::Future<folly::Unit>>(std::move(f));
  auto callback = [&, p_ptr = std::move(p_ptr), task, task_id]() mutable {
    p_ptr->setValue();
    return Status::OK();
  };
  return std::make_pair(std::any{std::move(f_ptr)}, std::move(callback));
}

}  // namespace ara::schedule::detail
