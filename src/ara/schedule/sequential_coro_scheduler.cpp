#include "sequential_coro_scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>
#include <ara/util/defines.h>

#include <algorithm>
#include <condition_variable>
#include <coroutine>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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

class SingleThreadResumer : public task::Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override {
    std::vector<Callback> callbacks;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (resumed_) {
        return;
      }
      resumed_ = true;
      callbacks.swap(callbacks_);
    }
    for (auto& cb : callbacks) {
      if (cb) {
        cb();
      }
    }
  }

  bool IsResumed() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return resumed_;
  }

  void AddCallback(Callback cb) {
    bool call_now = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (resumed_) {
        call_now = true;
      } else {
        callbacks_.push_back(std::move(cb));
      }
    }
    if (call_now && cb) {
      cb();
    }
  }

 private:
  std::mutex mutex_;
  bool resumed_ = false;
  std::vector<Callback> callbacks_;
};

class SingleThreadAwaiter : public task::Awaiter,
                            public std::enable_shared_from_this<SingleThreadAwaiter> {
 public:
  using ScheduleFn = std::function<void(std::coroutine_handle<>)>;

  explicit SingleThreadAwaiter(size_t num_readies) : num_readies_(num_readies) {}

  struct Awaitable {
    std::shared_ptr<SingleThreadAwaiter> awaiter;
    ScheduleFn schedule;

    bool await_ready() const noexcept { return awaiter->IsReady(); }

    bool await_suspend(std::coroutine_handle<> continuation) {
      return awaiter->Suspend(std::move(schedule), continuation);
    }

    void await_resume() const noexcept {}
  };

  Awaitable Await(ScheduleFn schedule) {
    return Awaitable{shared_from_this(), std::move(schedule)};
  }

  static std::shared_ptr<SingleThreadAwaiter> MakeSingle(task::ResumerPtr resumer) {
    task::Resumers resumers{std::move(resumer)};
    return MakeAwaiter(1, std::move(resumers));
  }

  static std::shared_ptr<SingleThreadAwaiter> MakeAny(task::Resumers resumers) {
    return MakeAwaiter(1, std::move(resumers));
  }

  static std::shared_ptr<SingleThreadAwaiter> MakeAll(task::Resumers resumers) {
    return MakeAwaiter(resumers.size(), std::move(resumers));
  }

  void NotifyReady() {
    ContinuationRecord record;
    bool should_schedule = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++readies_;
      if (readies_ < num_readies_) {
        return;
      }
      if (!record_.has_value() || scheduled_) {
        return;
      }
      scheduled_ = true;
      record = std::move(*record_);
      record_.reset();
      should_schedule = true;
    }
    if (should_schedule && record.continuation && record.schedule) {
      record.schedule(record.continuation);
    }
  }

 private:
  struct ContinuationRecord {
    std::coroutine_handle<> continuation;
    ScheduleFn schedule;
  };

  static std::shared_ptr<SingleThreadAwaiter> MakeAwaiter(size_t num_readies,
                                                         task::Resumers resumers) {
    auto awaiter = std::make_shared<SingleThreadAwaiter>(num_readies);
    for (auto& resumer : resumers) {
      auto casted = std::dynamic_pointer_cast<SingleThreadResumer>(resumer);
      ARA_CHECK(casted != nullptr);
      casted->AddCallback([awaiter]() { awaiter->NotifyReady(); });
    }
    return awaiter;
  }

  bool IsReady() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return readies_ >= num_readies_;
  }

  bool Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (readies_ >= num_readies_) {
      return false;
    }
    ARA_CHECK(!record_.has_value());
    record_.emplace(ContinuationRecord{continuation, std::move(schedule)});
    return true;
  }

  mutable std::mutex mutex_;
  size_t num_readies_;
  size_t readies_ = 0;
  std::optional<ContinuationRecord> record_;
  bool scheduled_ = false;
};

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

class SequentialCoroHandle;

struct QueueEntry {
  std::coroutine_handle<> handle;
  SequentialCoroHandle* owner;
};

class SequentialCoroEngine {
 public:
  struct YieldAwaiter {
    SequentialCoroEngine* engine;
    SequentialCoroHandle* owner;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
      engine->Enqueue({h, owner});
    }
    void await_resume() const noexcept {}
  };

  void Enqueue(QueueEntry entry) {
    ARA_CHECK(entry.handle);
    std::lock_guard<std::mutex> lock(mutex_);
    EnqueueLocked(entry);
    cv_.notify_one();
  }

  void Register(SequentialCoroHandle* handle);
  void Unregister(SequentialCoroHandle* handle);

  YieldAwaiter Yield(SequentialCoroHandle* owner) { return YieldAwaiter{this, owner}; }

  TaskResult WaitFor(SequentialCoroHandle* target);

 private:
  void RunLoop(SequentialCoroHandle* target);
  void FinalizeGroup(SequentialCoroHandle* owner);
  void EnqueueLocked(QueueEntry entry);
  void EnsureEnqueuedLocked();

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<QueueEntry> ready_;
  std::vector<SequentialCoroHandle*> handles_;
  bool running_ = false;
  std::thread::id runner_;
};

SequentialCoroEngine& Engine() {
  static SequentialCoroEngine engine;
  return engine;
}

class SequentialCoroHandle : public TaskGroupHandle {
 private:
  static const std::string kName;

 public:
  SequentialCoroHandle(const ScheduleContext& schedule_ctx, const TaskGroup& task_group,
                       TaskContext task_ctx)
      : TaskGroupHandle(kName, task_group, std::move(task_ctx)) {
    state_.schedule_ctx = &schedule_ctx;
    Initialize();
    Engine().Register(this);
  }

  ~SequentialCoroHandle() override { Engine().Unregister(this); }

 protected:
  TaskResult DoWait(const ScheduleContext& schedule_ctx) override;

 private:
  struct State {
    const ScheduleContext* schedule_ctx = nullptr;
    std::vector<TaskResult> results;
    std::vector<TaskCoro> coros;
    size_t finished = 0;
    bool completed = false;
    bool finalizing = false;
    bool enqueued = false;
    std::optional<TaskResult> final_result;
  };

  void Initialize();

  State state_;

  friend class SequentialCoroEngine;
  friend TaskCoro MakeTaskCoro(SequentialCoroHandle&, TaskId, TaskResult&);
};

const std::string SequentialCoroHandle::kName = "SequentialCoroHandle";

static TaskCoro MakeTaskCoro(SequentialCoroHandle& owner, TaskId task_id,
                             TaskResult& result) {
  auto schedule = [&owner](std::coroutine_handle<> h) {
    Engine().Enqueue({h, &owner});
  };

  const auto* schedule_ctx = owner.state_.schedule_ctx;
  ARA_CHECK(schedule_ctx != nullptr);
  const auto& task = owner.task_group_.GetTask();
  const auto& task_ctx = owner.task_ctx_;

  while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
    bool yielded = false;
    if (result->IsYield()) {
      yielded = true;
      if (schedule_ctx->schedule_observer != nullptr) {
        auto st = schedule_ctx->schedule_observer->Observe(
            &ScheduleObserver::OnTaskYield, *schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }
      co_await Engine().Yield(&owner);
    } else if (result->IsBlocked()) {
      if (schedule_ctx->schedule_observer != nullptr) {
        auto st = schedule_ctx->schedule_observer->Observe(
            &ScheduleObserver::OnTaskBlocked, *schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }

      auto awaiter = std::dynamic_pointer_cast<SingleThreadAwaiter>(result->GetAwaiter());
      ARA_CHECK(awaiter != nullptr);
      co_await awaiter->Await(schedule);

      if (schedule_ctx->schedule_observer != nullptr) {
        auto st = schedule_ctx->schedule_observer->Observe(
            &ScheduleObserver::OnTaskResumed, *schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }
    }

    result = task(task_ctx, task_id);

    if (yielded && result.ok() && !result->IsYield()) {
      if (schedule_ctx->schedule_observer != nullptr) {
        auto st = schedule_ctx->schedule_observer->Observe(
            &ScheduleObserver::OnTaskYieldBack, *schedule_ctx, task, task_id);
        if (!st.ok()) {
          result = std::move(st);
          co_return;
        }
      }
    }

    if (result.ok() && result->IsContinue()) {
      co_await Engine().Yield(&owner);
    }
  }
}

void SequentialCoroHandle::Initialize() {
  const auto num_tasks = task_group_.NumTasks();
  state_.results.assign(num_tasks, TaskStatus::Continue());
  state_.coros.reserve(num_tasks);
  for (TaskId i = 0; i < num_tasks; ++i) {
    state_.coros.push_back(MakeTaskCoro(*this, i, state_.results[i]));
  }
}

TaskResult SequentialCoroHandle::DoWait(const ScheduleContext&) {
  return Engine().WaitFor(this);
}

TaskResult SequentialCoroEngine::WaitFor(SequentialCoroHandle* target) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureEnqueuedLocked();
    cv_.notify_all();
    if (target->state_.completed) {
      return *target->state_.final_result;
    }
  }

  if (target->task_group_.NumTasks() == 0) {
    FinalizeGroup(target);
    std::unique_lock<std::mutex> lock(mutex_);
    return *target->state_.final_result;
  }

  bool owns_run = false;
  while (true) {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureEnqueuedLocked();
    cv_.notify_all();
    if (target->state_.completed) {
      return *target->state_.final_result;
    }
    if (!running_) {
      running_ = true;
      runner_ = std::this_thread::get_id();
      owns_run = true;
      break;
    }
    if (runner_ == std::this_thread::get_id()) {
      break;
    }
    cv_.wait(lock, [&]() { return target->state_.completed || !running_; });
  }

  RunLoop(target);

  if (owns_run) {
    std::unique_lock<std::mutex> lock(mutex_);
    running_ = false;
    runner_ = std::thread::id{};
    cv_.notify_all();
  }

  std::unique_lock<std::mutex> lock(mutex_);
  return *target->state_.final_result;
}

void SequentialCoroEngine::RunLoop(SequentialCoroHandle* target) {
  while (true) {
    QueueEntry entry;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      EnsureEnqueuedLocked();
      cv_.wait(lock, [&]() { return !ready_.empty() || target->state_.completed; });
      if (target->state_.completed) {
        return;
      }
      entry = ready_.front();
      ready_.pop_front();
    }

    entry.handle.resume();

    if (entry.handle.done()) {
      bool should_finalize = false;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!entry.owner->state_.completed) {
          entry.owner->state_.finished++;
          should_finalize =
              entry.owner->state_.finished >= entry.owner->task_group_.NumTasks();
        }
      }
      if (should_finalize) {
        FinalizeGroup(entry.owner);
      }
    }
  }
}

void SequentialCoroEngine::FinalizeGroup(SequentialCoroHandle* owner) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (owner->state_.completed || owner->state_.finalizing) {
      return;
    }
    owner->state_.finalizing = true;
  }

  TaskResult final = TaskStatus::Finished();
  const auto* schedule_ctx = owner->state_.schedule_ctx;
  const auto& task_group = owner->task_group_;

  if (schedule_ctx != nullptr && schedule_ctx->schedule_observer != nullptr) {
    auto status = schedule_ctx->schedule_observer->Observe(
        &ScheduleObserver::OnAllTasksFinished, *schedule_ctx, task_group,
        owner->state_.results);
    if (!status.ok()) {
      final = std::move(status);
    }
  }

  if (final.ok()) {
    for (auto& result : owner->state_.results) {
      if (!result.ok()) {
        final = result.status();
        break;
      }
    }
  }

  if (final.ok()) {
    const auto& cont = task_group.GetContinuation();
    if (cont.has_value()) {
      final = cont.value()(owner->task_ctx_);
    }
  }

  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!owner->state_.completed) {
      owner->state_.completed = true;
      owner->state_.final_result = std::move(final);
    }
    owner->state_.finalizing = false;
  }
  cv_.notify_all();
}

void SequentialCoroEngine::EnqueueLocked(QueueEntry entry) {
  ARA_CHECK(entry.handle);
  ready_.push_back(entry);
}

void SequentialCoroEngine::EnsureEnqueuedLocked() {
  for (auto* handle : handles_) {
    if (handle == nullptr || handle->state_.enqueued) {
      continue;
    }
    for (auto& coro : handle->state_.coros) {
      if (coro.handle) {
        EnqueueLocked({coro.handle, handle});
      }
    }
    handle->state_.enqueued = true;
  }
}

void SequentialCoroEngine::Register(SequentialCoroHandle* handle) {
  if (handle == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  handles_.push_back(handle);
  if (running_) {
    EnsureEnqueuedLocked();
    cv_.notify_all();
  }
}

void SequentialCoroEngine::Unregister(SequentialCoroHandle* handle) {
  if (handle == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  handles_.erase(std::remove(handles_.begin(), handles_.end(), handle),
                 handles_.end());
  ready_.erase(std::remove_if(ready_.begin(), ready_.end(),
                              [&](const QueueEntry& entry) {
                                return entry.owner == handle;
                              }),
               ready_.end());
}

}  // namespace

const std::string SequentialCoroScheduler::kName = "SequentialCoroScheduler";
const std::string SequentialCoroScheduler::kDesc =
    "Single-threaded scheduler that runs tasks cooperatively using C++20 coroutines";

Result<std::unique_ptr<TaskGroupHandle>> SequentialCoroScheduler::DoSchedule(
    const ScheduleContext& schedule_ctx, const TaskGroup& task_group) {
  auto task_ctx = MakeTaskContext(schedule_ctx);
  return std::make_unique<SequentialCoroHandle>(schedule_ctx, task_group,
                                                std::move(task_ctx));
}

ResumerFactory SequentialCoroScheduler::MakeResumerFactory(const ScheduleContext&) const {
  return []() -> Result<ResumerPtr> { return std::make_shared<SingleThreadResumer>(); };
}

SingleAwaiterFactory SequentialCoroScheduler::MakeSingleAwaiterFactgory(
    const ScheduleContext&) const {
  return SingleThreadAwaiter::MakeSingle;
}

AnyAwaiterFactory SequentialCoroScheduler::MakeAnyAwaiterFactgory(
    const ScheduleContext&) const {
  return SingleThreadAwaiter::MakeAny;
}

AllAwaiterFactory SequentialCoroScheduler::MakeAllAwaiterFactgory(
    const ScheduleContext&) const {
  return SingleThreadAwaiter::MakeAll;
}

}  // namespace ara::schedule
