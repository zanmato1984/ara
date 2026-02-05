#include "parallel_coro_scheduler.h"
#include "coro_ready_queue.h"
#include "coro_awaiter.h"
#include "coro_resumer.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>
#include <ara/util/defines.h>

#include <algorithm>
#include <atomic>
#include <coroutine>
#include <exception>
#include <functional>
#include <future>
#include <thread>
#include <vector>

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

namespace {

struct TaskCoro {
  struct promise_type {
    TaskCoro get_return_object() {
      return TaskCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  explicit TaskCoro(std::coroutine_handle<promise_type> handle) : handle(handle) {}

  TaskCoro(TaskCoro&& other) noexcept : handle(other.handle) { other.handle = {}; }
  TaskCoro& operator=(TaskCoro&& other) noexcept {
    if (this != &other) {
      if (handle) {
        handle.destroy();
      }
      handle = other.handle;
      other.handle = {};
    }
    return *this;
  }

  TaskCoro(const TaskCoro&) = delete;
  TaskCoro& operator=(const TaskCoro&) = delete;

  ~TaskCoro() {
    if (handle) {
      handle.destroy();
    }
  }

  std::coroutine_handle<promise_type> handle;
};

static size_t DefaultWorkerCount(size_t num_tasks) {
  size_t hw = static_cast<size_t>(std::thread::hardware_concurrency());
  if (hw == 0) {
    hw = 4;
  }
  return std::max<size_t>(1, std::min(num_tasks, hw));
}

}  // namespace

class ParallelCoroHandle : public TaskGroupHandle {
 private:
  static const std::string kName;

  using MakeFuture = std::function<std::future<TaskResult>(const TaskContext&)>;

 public:
  ParallelCoroHandle(const TaskGroup& task_group, TaskContext task_ctx,
                     MakeFuture make_future)
      : TaskGroupHandle(kName, task_group, std::move(task_ctx)),
        future_(make_future(task_ctx_)) {}

 protected:
  TaskResult DoWait(const ScheduleContext&) override;

 private:
  std::future<TaskResult> future_;
};

const std::string ParallelCoroHandle::kName = "ParallelCoroHandle";

static TaskCoro MakeTaskCoro(detail::CoroReadyQueue& queue,
                             const ScheduleContext& schedule_ctx, const Task& task,
                             const TaskContext& task_ctx, TaskId task_id,
                             TaskResult& result) {
  auto schedule = [&queue](std::coroutine_handle<> h) { queue.Enqueue(h); };

  while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
    bool yielded = false;
    if (result->IsYield()) {
      yielded = true;
      if (schedule_ctx.schedule_observer != nullptr) {
        auto st = schedule_ctx.schedule_observer->Observe(&ScheduleObserver::OnTaskYield,
                                                          schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }
      co_await queue.Yield();
    } else if (result->IsBlocked()) {
      if (schedule_ctx.schedule_observer != nullptr) {
        auto st = schedule_ctx.schedule_observer->Observe(
            &ScheduleObserver::OnTaskBlocked, schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }

      auto awaiter = std::dynamic_pointer_cast<CoroAwaiter>(result->GetAwaiter());
      ARA_CHECK(awaiter != nullptr);
      co_await awaiter->Await(schedule);

      if (schedule_ctx.schedule_observer != nullptr) {
        auto st = schedule_ctx.schedule_observer->Observe(
            &ScheduleObserver::OnTaskResumed, schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }
    }

    result = task(task_ctx, task_id);

    if (yielded && result.ok() && !result->IsYield()) {
      if (schedule_ctx.schedule_observer != nullptr) {
        auto st = schedule_ctx.schedule_observer->Observe(
            &ScheduleObserver::OnTaskYieldBack, schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }
    }
  }
}

TaskResult ParallelCoroHandle::DoWait(const ScheduleContext&) { return future_.get(); }

const std::string ParallelCoroScheduler::kName = "ParallelCoroScheduler";
const std::string ParallelCoroScheduler::kDesc =
    "Thread-pool scheduler that runs tasks cooperatively using C++20 coroutines";

Result<std::unique_ptr<TaskGroupHandle>> ParallelCoroScheduler::DoSchedule(
    const ScheduleContext& schedule_ctx, const TaskGroup& task_group) {
  auto make_future = [&](const TaskContext& task_ctx) -> std::future<TaskResult> {
    return std::async(std::launch::async, [&]() -> TaskResult {
      const auto num_tasks = task_group.NumTasks();
      const auto& task = task_group.GetTask();
      const auto& cont = task_group.GetContinuation();

      std::vector<TaskResult> results(num_tasks, TaskStatus::Continue());
      if (num_tasks == 0) {
        if (schedule_ctx.schedule_observer != nullptr) {
          auto status = schedule_ctx.schedule_observer->Observe(
              &ScheduleObserver::OnAllTasksFinished, schedule_ctx, task_group, results);
          if (!status.ok()) {
            return std::move(status);
          }
        }
        if (cont.has_value()) {
          return cont.value()(task_ctx);
        }
        return TaskStatus::Finished();
      }

      detail::CoroReadyQueue queue(num_tasks);

      std::vector<TaskCoro> coros;
      coros.reserve(num_tasks);
      for (TaskId i = 0; i < num_tasks; ++i) {
        coros.push_back(MakeTaskCoro(queue, schedule_ctx, task, task_ctx, i, results[i]));
        queue.Enqueue(coros.back().handle);
      }

      std::atomic<size_t> finished = 0;
      auto worker = [&]() {
        while (true) {
          auto h = queue.DequeueOrWait(
              [&]() { return finished.load(std::memory_order_acquire) >= num_tasks; });
          if (!h) {
            break;
          }

          h.resume();

          if (h.done()) {
            auto new_finished = finished.fetch_add(1) + 1;
            if (new_finished >= num_tasks) {
              queue.WakeAll();
            }
          }
        }
      };

      const auto num_workers = DefaultWorkerCount(num_tasks);
      std::vector<std::jthread> threads;
      threads.reserve(num_workers);
      for (size_t i = 0; i < num_workers; ++i) {
        threads.emplace_back(worker);
      }

      for (auto& thread : threads) {
        thread.join();
      }

      if (schedule_ctx.schedule_observer != nullptr) {
        auto status = schedule_ctx.schedule_observer->Observe(
            &ScheduleObserver::OnAllTasksFinished, schedule_ctx, task_group, results);
        if (!status.ok()) {
          return std::move(status);
        }
      }

      for (auto& result : results) {
        ARA_RETURN_NOT_OK(result);
      }

      if (cont.has_value()) {
        return cont.value()(task_ctx);
      }
      return TaskStatus::Finished();
    });
  };

  auto task_ctx = MakeTaskContext(schedule_ctx);
  return std::make_unique<ParallelCoroHandle>(task_group, std::move(task_ctx),
                                              std::move(make_future));
}

ResumerFactory ParallelCoroScheduler::MakeResumerFactory(const ScheduleContext&) const {
  return []() -> Result<ResumerPtr> { return std::make_shared<CoroResumer>(); };
}

SingleAwaiterFactory ParallelCoroScheduler::MakeSingleAwaiterFactgory(
    const ScheduleContext&) const {
  return CoroAwaiter::MakeSingle;
}

AnyAwaiterFactory ParallelCoroScheduler::MakeAnyAwaiterFactgory(
    const ScheduleContext&) const {
  return CoroAwaiter::MakeAny;
}

AllAwaiterFactory ParallelCoroScheduler::MakeAllAwaiterFactgory(
    const ScheduleContext&) const {
  return CoroAwaiter::MakeAll;
}

}  // namespace ara::schedule
