#include "async_dual_pool_scheduler.h"
#include "async_awaiter.h"
#include "async_resumer.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/util/util.h>

namespace ara::schedule {

using detail::Future;
using detail::Promise;
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

const std::string AsyncHandle::kName = "AsyncHandle";

TaskResult AsyncHandle::DoWait(const ScheduleContext&) { return future_.wait().value(); }

const std::string AsyncDualPoolScheduler::kName = "AsyncDualPoolScheduler";
const std::string AsyncDualPoolScheduler::kDesc =
    "Scheduler that uses folly::future for task parallelism and dual thread pool "
    "executors (folly::CPU/IOThreadPoolExecutor) for CPU/IO tasks";

Result<std::unique_ptr<TaskGroupHandle>> AsyncDualPoolScheduler::DoSchedule(
    const ScheduleContext& schedule_ctx, const TaskGroup& task_group) {
  auto& task = task_group.GetTask();
  auto num_tasks = task_group.NumTasks();
  auto& cont = task_group.GetContinuation();

  auto make_future = [&](const TaskContext& task_ctx,
                         std::vector<TaskResult>& results) -> Future {
    std::vector<Promise> task_promises;
    std::vector<Future> task_futures;
    for (size_t i = 0; i < num_tasks; ++i) {
      auto [tp, tf] = MakeTask(schedule_ctx, task, task_ctx, i, results[i]);
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
                 .thenValue([&schedule_ctx, &task_group, &cont,
                             &task_ctx](auto&& try_results) -> TaskResult {
                   std::vector<TaskResult> results(try_results.size());
                   std::transform(try_results.begin(), try_results.end(), results.begin(),
                                  [](auto&& try_result) -> TaskResult {
                                    ARA_CHECK(try_result.hasValue());
                                    return try_result.value();
                                  });
                   if (schedule_ctx.schedule_observer != nullptr) {
                     auto status = schedule_ctx.schedule_observer->Observe(
                         &ScheduleObserver::OnAllTasksFinished, schedule_ctx, task_group,
                         results);
                     if (!status.ok()) {
                       return std::move(status);
                     }
                   }
                   for (auto&& result : results) {
                     ARA_RETURN_NOT_OK(result);
                   }
                   if (cont.has_value()) {
                     return cont.value()(task_ctx);
                   }
                   return TaskStatus::Finished();
                 });
    p.setValue();
    return std::move(f);
  };
  TaskContext task_ctx = MakeTaskContext(schedule_ctx);
  std::vector<TaskResult> results(num_tasks);

  return std::make_unique<AsyncHandle>(task_group, std::move(task_ctx),
                                       std::move(results), std::move(make_future));
}

ResumerFactory AsyncDualPoolScheduler::MakeResumerFactory(const ScheduleContext&) const {
  return []() -> Result<ResumerPtr> { return std::make_shared<AsyncResumer>(); };
}

SingleAwaiterFactory AsyncDualPoolScheduler::MakeSingleAwaiterFactgory(
    const ScheduleContext&) const {
  return AsyncAwaiter::MakeSingle;
}

AnyAwaiterFactory AsyncDualPoolScheduler::MakeAnyAwaiterFactgory(
    const ScheduleContext&) const {
  return AsyncAwaiter::MakeAny;
}

AllAwaiterFactory AsyncDualPoolScheduler::MakeAllAwaiterFactgory(
    const ScheduleContext&) const {
  return AsyncAwaiter::MakeAll;
}

AsyncDualPoolScheduler::ConcreteTask AsyncDualPoolScheduler::MakeTask(
    const ScheduleContext& schedule_ctx, const Task& task, const TaskContext& context,
    TaskId task_id, TaskResult& result) const {
  auto [p, f] = folly::makePromiseContract<folly::Unit>(cpu_executor_);
  auto pred = [&]() {
    return result.ok() && !result->IsFinished() && !result->IsCancelled();
  };
  auto thunk = [&, task_id]() {
    if (result->IsBlocked()) {
      if (schedule_ctx.schedule_observer != nullptr) {
        auto status = schedule_ctx.schedule_observer->Observe(
            &ScheduleObserver::OnTaskBlocked, schedule_ctx, task, task_id);
        if (!status.ok()) {
          result = std::move(status);
          return folly::makeFuture();
        }
      }
      auto awaiter = std::dynamic_pointer_cast<AsyncAwaiter>(result->GetAwaiter());
      return std::move(awaiter->GetFuture())
          .via(cpu_executor_)
          .thenValue([&, task_id](auto&&) {
            if (schedule_ctx.schedule_observer != nullptr) {
              auto status = schedule_ctx.schedule_observer->Observe(
                  &ScheduleObserver::OnTaskResumed, schedule_ctx, task, task_id);
              if (!status.ok()) {
                result = std::move(status);
                return;
              }
            }
            result = TaskStatus::Continue();
          });
    }

    if (result->IsYield()) {
      if (schedule_ctx.schedule_observer != nullptr) {
        auto status = schedule_ctx.schedule_observer->Observe(
            &ScheduleObserver::OnTaskYield, schedule_ctx, task, task_id);
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
        if (schedule_ctx.schedule_observer != nullptr) {
          auto status = schedule_ctx.schedule_observer->Observe(
              &ScheduleObserver::OnTaskYieldBack, schedule_ctx, task, task_id);
          if (!status.ok()) {
            result = std::move(status);
          }
        }
      });
    }

    folly::Executor* executor =
        task.Hint().type == task::TaskHint::Type::CPU ? cpu_executor_ : io_executor_;
    return folly::via(executor).then(
        [&, task_id](auto&&) { result = task(context, task_id); });
  };

  auto task_f =
      std::move(f).thenValue([pred, thunk, &result](auto&&) -> folly::Future<TaskResult> {
        return folly::whileDo(pred, thunk).thenValue([&](auto&&) {
          return std::move(result);
        });
      });

  return {std::move(p), std::move(task_f)};
}

}  // namespace ara::schedule
