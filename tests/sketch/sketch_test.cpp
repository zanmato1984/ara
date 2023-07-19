#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>
#include <stack>

#define ARRA_DCHECK ARROW_DCHECK
#define ARRA_RETURN_NOT_OK ARROW_RETURN_NOT_OK
#define ARRA_ASSIGN_OR_RAISE ARROW_ASSIGN_OR_RAISE

namespace arra::sketch {

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BACKPRESSURE,
    YIELD,
    FINISHED,
    // TODO: May need a "REPEAT" status to support join spill, i.e., restore
    // sub-hashtables probe side batch partitions, and redo join for this partition.
    CANCELLED,
  } code_;

  TaskStatus(Code code) : code_(code) {}

 public:
  bool IsContinue() { return code_ == Code::CONTINUE; }
  bool IsBackpressure() { return code_ == Code::BACKPRESSURE; }
  bool IsYield() { return code_ == Code::YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

  std::string ToString() {
    switch (code_) {
      case Code::CONTINUE:
        return "CONTINUE";
      case Code::BACKPRESSURE:
        return "BACKPRESSURE";
      case Code::YIELD:
        return "YIELD";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
      default:
        return "UNKNOWN";
    }
  }

 public:
  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Backpressure() { return TaskStatus{Code::BACKPRESSURE}; }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};

using TaskId = size_t;
using ThreadId = size_t;

using TaskResult = arrow::Result<TaskStatus>;
using Task = std::function<TaskResult(TaskId)>;
using TaskCont = std::function<TaskResult()>;
using TaskGroup = std::tuple<Task, size_t, std::optional<TaskCont>>;
using TaskGroups = std::vector<TaskGroup>;

using Batch = std::vector<int>;

struct OperatorResult {
 private:
  enum class OperatorStatus {
    NEEDS_MORE,
    EVEN,
    HAS_MORE,
    BACKPRESSURE,
    YIELD,
    FINISHED,
    CANCELLED,
  } status_;
  std::optional<Batch> output_;

  OperatorResult(OperatorStatus status, std::optional<Batch> output = std::nullopt)
      : status_(status), output_(std::move(output)) {}

 public:
  bool IsNeedsMore() { return status_ == OperatorStatus::NEEDS_MORE; }
  bool IsEven() { return status_ == OperatorStatus::EVEN; }
  bool IsHasMore() { return status_ == OperatorStatus::HAS_MORE; }
  bool IsBackpressure() { return status_ == OperatorStatus::BACKPRESSURE; }
  bool IsYield() { return status_ == OperatorStatus::YIELD; }
  bool IsFinished() { return status_ == OperatorStatus::FINISHED; }
  bool IsCancelled() { return status_ == OperatorStatus::CANCELLED; }

  std::optional<Batch>& GetOutput() {
    ARRA_DCHECK(IsEven() || IsHasMore() || IsFinished());
    return output_;
  }

 public:
  static OperatorResult NeedsMore() { return OperatorResult(OperatorStatus::NEEDS_MORE); }
  static OperatorResult Even(Batch output) {
    return OperatorResult(OperatorStatus::EVEN, std::move(output));
  }
  static OperatorResult HasMore(Batch output) {
    return OperatorResult{OperatorStatus::HAS_MORE, std::move(output)};
  }
  static OperatorResult Backpressure() {
    return OperatorResult{OperatorStatus::BACKPRESSURE};
  }
  static OperatorResult Yield() { return OperatorResult{OperatorStatus::YIELD}; }
  static OperatorResult Finished(std::optional<Batch> output) {
    return OperatorResult{OperatorStatus::FINISHED, std::move(output)};
  }
  static OperatorResult Cancelled() { return OperatorResult{OperatorStatus::CANCELLED}; }
};

using PipelineTaskSource = std::function<arrow::Result<OperatorResult>(ThreadId)>;
using PipelineTaskPipe =
    std::function<arrow::Result<OperatorResult>(ThreadId, std::optional<Batch>)>;
using PipelineTaskDrain = std::function<arrow::Result<OperatorResult>(ThreadId)>;
using PipelineTaskSink =
    std::function<arrow::Result<OperatorResult>(ThreadId, std::optional<Batch>)>;

class SourceOp {
 public:
  virtual ~SourceOp() = default;
  virtual PipelineTaskSource Source() = 0;
  virtual TaskGroups Frontend() = 0;
  // TODO: How does backend go to possibly IO executor.
  virtual TaskGroups Backend() = 0;
};

class PipeOp {
 public:
  virtual ~PipeOp() = default;
  virtual PipelineTaskPipe Pipe() = 0;
  virtual std::optional<PipelineTaskDrain> Drain() = 0;
  virtual std::unique_ptr<SourceOp> Source() = 0;
};

class SinkOp {
 public:
  virtual ~SinkOp() = default;
  virtual PipelineTaskSink Sink() = 0;
  virtual TaskGroups Frontend() = 0;
  // TODO: How does backend go to possibly IO executor.
  virtual TaskGroups Backend() = 0;
};

struct Pipeline {
  SourceOp* source;
  std::vector<PipeOp*> pipes;
  SinkOp* sink;
};

class PipelineTask {
 public:
  PipelineTask(
      size_t dop, const PipelineTaskSource& source,
      const std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>>&
          pipes,
      const PipelineTaskSink& sink)
      : dop_(dop), source_(source), pipes_(pipes), sink_(sink), local_states_(dop) {
    std::vector<size_t> drains;
    for (size_t i = 0; i < pipes_.size(); ++i) {
      if (pipes_[i].second.has_value()) {
        drains.push_back(i);
      }
    }
    for (size_t i = 0; i < dop; ++i) {
      local_states_[i].drains = drains;
    }
  }

  TaskResult Run(ThreadId thread_id) {
    if (cancelled) {
      return TaskStatus::Cancelled();
    }

    if (local_states_[thread_id].backpressure) {
      auto result = sink_(thread_id, std::nullopt);
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      ARRA_DCHECK(result->IsNeedsMore() || result->IsBackpressure());
      if (!result->IsBackpressure()) {
        local_states_[thread_id].backpressure = false;
        return TaskStatus::Continue();
      }
      return TaskStatus::Backpressure();
    }

    if (!local_states_[thread_id].pipe_stack.empty()) {
      auto pipe_id = local_states_[thread_id].pipe_stack.top();
      local_states_[thread_id].pipe_stack.pop();
      return Pipe(thread_id, pipe_id, std::nullopt);
    }

    if (!local_states_[thread_id].source_done) {
      auto result = source_(thread_id);
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (result->IsFinished()) {
        local_states_[thread_id].source_done = true;
        if (result->GetOutput().has_value()) {
          return Pipe(thread_id, 0, std::move(result->GetOutput()));
        }
      } else {
        ARRA_DCHECK(result->IsHasMore());
        ARRA_DCHECK(result->GetOutput().has_value());
        return Pipe(thread_id, 0, std::move(result->GetOutput()));
      }
    }

    if (local_states_[thread_id].draining >= local_states_[thread_id].drains.size()) {
      return TaskStatus::Finished();
    }

    for (; local_states_[thread_id].draining < local_states_[thread_id].drains.size();
         ++local_states_[thread_id].draining) {
      auto drain_id = local_states_[thread_id].drains[local_states_[thread_id].draining];
      auto result = pipes_[drain_id].second.value()(thread_id);
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (local_states_[thread_id].yield) {
        ARRA_DCHECK(result->IsNeedsMore());
        local_states_[thread_id].yield = false;
        return TaskStatus::Continue();
      }
      if (result->IsYield()) {
        ARRA_DCHECK(!local_states_[thread_id].yield);
        local_states_[thread_id].yield = true;
        return TaskStatus::Yield();
      }
      ARRA_DCHECK(result->IsHasMore() || result->IsFinished());
      if (result->GetOutput().has_value()) {
        return Pipe(thread_id, drain_id + 1, std::move(result->GetOutput()));
      }
    }

    return TaskStatus::Finished();
  }

 private:
  TaskResult Pipe(ThreadId thread_id, size_t pipe_id, std::optional<Batch> input) {
    for (size_t i = pipe_id; i < pipes_.size(); i++) {
      auto result = pipes_[i].first(thread_id, std::move(input));
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (local_states_[thread_id].yield) {
        ARRA_DCHECK(result->IsNeedsMore());
        local_states_[thread_id].pipe_stack.push(i);
        local_states_[thread_id].yield = false;
        return TaskStatus::Continue();
      }
      if (result->IsYield()) {
        ARRA_DCHECK(!local_states_[thread_id].yield);
        local_states_[thread_id].pipe_stack.push(i);
        local_states_[thread_id].yield = true;
        return TaskStatus::Yield();
      }
      ARRA_DCHECK(result->IsNeedsMore() || result->IsEven() || result->IsHasMore());
      if (result->IsEven() || result->IsHasMore()) {
        if (result->IsHasMore()) {
          local_states_[thread_id].pipe_stack.push(i);
        }
        ARRA_DCHECK(result->GetOutput().has_value());
        input = std::move(result->GetOutput());
      } else {
        return TaskStatus::Continue();
      }
    }

    auto result = sink_(thread_id, std::move(input));
    if (!result.ok()) {
      cancelled = true;
      return result.status();
    }
    ARRA_DCHECK(result->IsNeedsMore() || result->IsBackpressure());
    if (result->IsBackpressure()) {
      local_states_[thread_id].backpressure = true;
      return TaskStatus::Backpressure();
    }
    return TaskStatus::Continue();
  }

 private:
  size_t dop_;
  // TODO:: Multi-source.
  PipelineTaskSource source_;
  std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>> pipes_;
  PipelineTaskSink sink_;

  struct ThreadLocalState {
    std::stack<size_t> pipe_stack;
    bool source_done = false;
    std::vector<size_t> drains;
    size_t draining = 0;
    bool backpressure = false, yield = false;
  };
  std::vector<ThreadLocalState> local_states_;
  std::atomic_bool cancelled = false;
};

template <typename Scheduler>
class Driver {
 public:
  Driver(std::vector<Pipeline> pipelines, Scheduler* scheduler)
      : pipelines_(std::move(pipelines)), scheduler_(scheduler) {}

  TaskResult Run(size_t dop) {
    for (const auto& pipeline : pipelines_) {
      ARRA_ASSIGN_OR_RAISE(
          auto result, RunPipeline(dop, pipeline.source, pipeline.pipes, pipeline.sink));
      ARRA_DCHECK(result.IsFinished());
    }
    return TaskStatus::Finished();
  }

 private:
  TaskResult RunPipeline(size_t dop, SourceOp* source, const std::vector<PipeOp*>& pipes,
                         SinkOp* sink) {
    // TODO: Backend should be waited even error happens.
    auto sink_be = sink->Backend();
    auto sink_be_handle = scheduler_->ScheduleTaskGroups(sink_be);

    std::vector<std::unique_ptr<SourceOp>> sources_keepalive;
    std::vector<std::pair<SourceOp*, size_t>> sources;
    sources.emplace_back(source, 0);
    for (size_t i = 0; i < pipes.size(); i++) {
      if (auto pipe_source = pipes[i]->Source(); pipe_source != nullptr) {
        sources.emplace_back(pipe_source.get(), i + 1);
        sources_keepalive.emplace_back(std::move(pipe_source));
      }
    }

    for (const auto& [source, pipe_start] : sources) {
      ARRA_ASSIGN_OR_RAISE(auto result,
                           RunPipeline(dop, source, pipes, pipe_start, sink));
      ARRA_DCHECK(result.IsFinished());
    }

    auto sink_fe = sink->Frontend();
    auto sink_fe_handle = scheduler_->ScheduleTaskGroups(sink_fe);
    ARRA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroups(sink_fe_handle));
    ARRA_DCHECK(result.IsFinished());

    ARRA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroups(sink_be_handle));
    ARRA_DCHECK(result.IsFinished());

    return TaskStatus::Finished();
  }

  TaskResult RunPipeline(size_t dop, SourceOp* source, const std::vector<PipeOp*>& pipes,
                         size_t pipe_start, SinkOp* sink) {
    // TODO: Backend should be waited even error happens.
    auto source_be = source->Backend();
    auto source_be_handle = scheduler_->ScheduleTaskGroups(source_be);

    auto source_fe = source->Frontend();
    auto source_fe_handle = scheduler_->ScheduleTaskGroups(source_fe);
    ARRA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroups(source_fe_handle));
    ARRA_DCHECK(result.IsFinished());

    auto source_source = source->Source();
    std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>>
        pipe_and_drains;
    for (size_t i = pipe_start; i < pipes.size(); ++i) {
      auto pipe_pipe = pipes[i]->Pipe();
      auto pipe_drain = pipes[i]->Drain();
      pipe_and_drains.emplace_back(std::move(pipe_pipe), std::move(pipe_drain));
    }
    auto sink_sink = sink->Sink();
    PipelineTask pipeline_task(dop, source_source, pipe_and_drains, sink_sink);
    TaskGroup pipeline{[&](ThreadId thread_id) { return pipeline_task.Run(thread_id); },
                       dop, std::nullopt};
    auto pipeline_handle = scheduler_->ScheduleTaskGroup(pipeline);
    ARRA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroup(pipeline_handle));
    ARRA_DCHECK(result.IsFinished());

    ARRA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroups(source_be_handle));
    ARRA_DCHECK(result.IsFinished());

    return TaskStatus::Finished();
  }

 private:
  std::vector<Pipeline> pipelines_;
  Scheduler* scheduler_;
};

class FollyFutureScheduler {
 private:
  using ConcreteTask = folly::SemiFuture<TaskResult>;
  using TaskGroupPayload = std::vector<TaskResult>;

 public:
  using TaskGroupHandle = std::pair<folly::Future<TaskResult>, TaskGroupPayload>;
  using TaskGroupsHandle = std::vector<TaskGroupHandle>;

  FollyFutureScheduler(size_t num_threads) : executor_(num_threads) {}

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group) {
    auto& task = std::get<0>(group);
    auto num_tasks = std::get<1>(group);
    auto& task_cont = std::get<2>(group);

    TaskGroupHandle handle{folly::makeFuture(TaskStatus::Finished()),
                           TaskGroupPayload(num_tasks)};
    std::vector<ConcreteTask> tasks;
    for (size_t i = 0; i < num_tasks; ++i) {
      handle.second[i] = TaskStatus::Continue();
      tasks.push_back(MakeTask(task, i, handle.second[i]));
    }
    handle.first = folly::via(&executor_)
                       .thenValue([tasks = std::move(tasks)](auto&&) mutable {
                         return folly::collectAll(tasks);
                       })
                       .thenValue([&task_cont](auto&& try_results) -> TaskResult {
                         for (auto&& try_result : try_results) {
                           ARRA_DCHECK(try_result.hasValue());
                           auto result = try_result.value();
                           ARRA_RETURN_NOT_OK(result);
                         }
                         if (task_cont.has_value()) {
                           return task_cont.value()();
                         }
                         return TaskStatus::Finished();
                       });
    return std::move(handle);
  }

  TaskGroupsHandle ScheduleTaskGroups(const TaskGroups& groups) {
    TaskGroupsHandle handles;
    for (const auto& group : groups) {
      handles.push_back(ScheduleTaskGroup(group));
    }
    return handles;
  }

  TaskResult WaitTaskGroup(TaskGroupHandle& group) { return group.first.wait().value(); }

  TaskResult WaitTaskGroups(TaskGroupsHandle& groups) {
    for (auto& group : groups) {
      ARRA_RETURN_NOT_OK(WaitTaskGroup(group));
    }
    return TaskStatus::Finished();
  }

 private:
  static ConcreteTask MakeTask(const Task& task, TaskId task_id, TaskResult& result) {
    auto pred = [&]() {
      return result.ok() &&
             (result->IsContinue() || result->IsBackpressure() || result->IsYield());
    };
    auto thunk = [&, task_id]() {
      return folly::makeSemiFuture().defer(
          [&, task_id](auto&&) { result = task(task_id); });
    };
    return folly::whileDo(pred, thunk).deferValue([&](auto&&) {
      return std::move(result);
    });
  }

 private:
  folly::CPUThreadPoolExecutor executor_;
};

class FollyFutureDoublePoolScheduler {
 private:
  using ConcreteTask = folly::Future<TaskResult>;
  using TaskGroupPayload = std::vector<TaskResult>;

 public:
  using TaskGroupHandle = std::pair<folly::Future<TaskResult>, TaskGroupPayload>;
  using TaskGroupsHandle = std::vector<TaskGroupHandle>;

  class TaskObserver {
   public:
    virtual ~TaskObserver() = default;

    virtual void BeforeTaskRun(const Task& task, TaskId task_id) = 0;
    virtual void AfterTaskRun(const Task& task, TaskId task_id,
                              const TaskResult& result) = 0;
  };

  FollyFutureDoublePoolScheduler(folly::Executor* cpu_executor,
                                 folly::Executor* io_executor,
                                 TaskObserver* observer = nullptr)
      : cpu_executor_(cpu_executor), io_executor_(io_executor), observer_(observer) {}

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group) {
    auto& task = std::get<0>(group);
    auto num_tasks = std::get<1>(group);
    auto& task_cont = std::get<2>(group);

    TaskGroupHandle handle{folly::makeFuture(TaskStatus::Finished()),
                           TaskGroupPayload(num_tasks)};
    std::vector<ConcreteTask> tasks;
    for (size_t i = 0; i < num_tasks; ++i) {
      handle.second[i] = TaskStatus::Continue();
      tasks.push_back(MakeTask(task, i, handle.second[i]));
    }
    handle.first = folly::via(cpu_executor_)
                       .thenValue([tasks = std::move(tasks)](auto&&) mutable {
                         return folly::collectAll(tasks);
                       })
                       .thenValue([&task_cont](auto&& try_results) -> TaskResult {
                         for (auto&& try_result : try_results) {
                           ARRA_DCHECK(try_result.hasValue());
                           auto result = try_result.value();
                           ARRA_RETURN_NOT_OK(result);
                         }
                         if (task_cont.has_value()) {
                           return task_cont.value()();
                         }
                         return TaskStatus::Finished();
                       });
    return std::move(handle);
  }

  TaskGroupsHandle ScheduleTaskGroups(const TaskGroups& groups) {
    TaskGroupsHandle handles;
    for (const auto& group : groups) {
      handles.push_back(ScheduleTaskGroup(group));
    }
    return handles;
  }

  TaskResult WaitTaskGroup(TaskGroupHandle& group) { return group.first.wait().value(); }

  TaskResult WaitTaskGroups(TaskGroupsHandle& groups) {
    for (auto& group : groups) {
      ARRA_RETURN_NOT_OK(WaitTaskGroup(group));
    }
    return TaskStatus::Finished();
  }

 private:
  ConcreteTask MakeTask(const Task& task, TaskId task_id, TaskResult& result) {
    auto pred = [&]() {
      return result.ok() && !result->IsFinished() && !result->IsCancelled();
    };
    auto thunk = [&, task_id]() {
      auto* executor = result->IsYield() ? io_executor_ : cpu_executor_;
      return folly::via(executor).then([&, task_id](auto&&) {
        if (observer_) {
          observer_->BeforeTaskRun(task, task_id);
        }
        result = task(task_id);
        if (observer_) {
          observer_->AfterTaskRun(task, task_id, result);
        }
      });
    };
    return folly::whileDo(pred, thunk).thenValue([&](auto&&) {
      return std::move(result);
    });
  }

 private:
  folly::Executor* cpu_executor_;
  folly::Executor* io_executor_;
  TaskObserver* observer_;
};

}  // namespace arra::sketch

using namespace arra::sketch;

class MemorySource : public SourceOp {
 public:
  MemorySource(std::list<Batch> batches) : batches_(std::move(batches)) {}

  PipelineTaskSource Source() override {
    return [&](ThreadId) -> arrow::Result<OperatorResult> {
      std::lock_guard<std::mutex> lock(mutex_);
      if (batches_.empty()) {
        return OperatorResult::Finished(std::nullopt);
      }
      auto output = std::move(batches_.front());
      batches_.pop_front();
      if (batches_.empty()) {
        return OperatorResult::Finished(std::move(output));
      } else {
        return OperatorResult::HasMore(std::move(output));
      }
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 public:
  std::mutex mutex_;
  std::list<Batch> batches_;
};

class DistributedMemorySource : public SourceOp {
 public:
  DistributedMemorySource(size_t dop, std::list<Batch> batches) : dop_(dop) {
    thread_locals_.resize(dop_);
    for (auto& tl : thread_locals_) {
      tl.batches_ = batches;
    }
  }

  PipelineTaskSource Source() override {
    return [&](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].batches_.empty()) {
        return OperatorResult::Finished(std::nullopt);
      }
      auto output = std::move(thread_locals_[thread_id].batches_.front());
      thread_locals_[thread_id].batches_.pop_front();
      if (thread_locals_[thread_id].batches_.empty()) {
        return OperatorResult::Finished(std::move(output));
      } else {
        return OperatorResult::HasMore(std::move(output));
      }
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  size_t dop_;
  struct ThreadLocal {
    std::list<Batch> batches_;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class MemorySink : public SinkOp {
 public:
  PipelineTaskSink Sink() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        std::lock_guard<std::mutex> lock(mutex_);
        batches_.push_back(std::move(input.value()));
      }
      return OperatorResult::NeedsMore();
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 public:
  std::mutex mutex_;
  std::vector<Batch> batches_;
};

class IdentityPipe : public PipeOp {
 public:
  PipelineTaskPipe Pipe() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        return OperatorResult::Even(std::move(input.value()));
      }
      return OperatorResult::NeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }
};

TEST(SketchTest, OneToOne) {
  size_t dop = 8;
  MemorySource source({{1}});
  IdentityPipe pipe;
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 1);
  ASSERT_EQ(sink.batches_[0], (Batch{1}));
}

class TimesNFlatPipe : public PipeOp {
 public:
  TimesNFlatPipe(size_t n) : n_(n) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        auto& batch = input.value();
        Batch output(batch.size() * n_);
        for (size_t i = 0; i < n_; ++i) {
          std::copy(batch.begin(), batch.end(), output.begin() + i * batch.size());
        }
        return OperatorResult::Even(std::move(output));
      }
      return OperatorResult::NeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  size_t n_;
};

TEST(SketchTest, OneToThreeFlat) {
  size_t dop = 8;
  MemorySource source({{1}});
  TimesNFlatPipe pipe(3);
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 1);
  ASSERT_EQ(sink.batches_[0], (Batch{1, 1, 1}));
}

class TimesNSlicedPipe : public PipeOp {
 public:
  TimesNSlicedPipe(size_t dop, size_t n) : dop_(dop), n_(n) {
    thread_locals_.resize(dop_);
  }

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].batches.empty()) {
        ARRA_DCHECK(input.has_value());
        for (size_t i = 0; i < n_; i++) {
          thread_locals_[thread_id].batches.push_back(input.value());
        }
      } else {
        ARRA_DCHECK(!input.has_value());
      }
      auto output = std::move(thread_locals_[thread_id].batches.front());
      thread_locals_[thread_id].batches.pop_front();
      if (thread_locals_[thread_id].batches.empty()) {
        return OperatorResult::Even(std::move(output));
      } else {
        return OperatorResult::HasMore(std::move(output));
      }
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  size_t dop_, n_;

 private:
  struct ThreadLocal {
    std::list<Batch> batches;
  };
  std::vector<ThreadLocal> thread_locals_;
};

TEST(SketchTest, OneToThreeSliced) {
  size_t dop = 8;
  MemorySource source({{1}});
  TimesNSlicedPipe pipe(dop, 3);
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 3);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(sink.batches_[i], (Batch{1}));
  }
}

class AccumulatePipe : public PipeOp {
 public:
  AccumulatePipe(size_t dop, size_t n) : dop_(dop), n_(n) { thread_locals_.resize(dop_); }

  PipelineTaskPipe Pipe() override {
    PipelineTaskPipe f =
        [&](ThreadId thread_id,
            std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].batch.size() >= n_) {
        ARRA_DCHECK(!input.has_value());
        Batch output(thread_locals_[thread_id].batch.begin(),
                     thread_locals_[thread_id].batch.begin() + n_);
        thread_locals_[thread_id].batch =
            Batch(thread_locals_[thread_id].batch.begin() + n_,
                  thread_locals_[thread_id].batch.end());
        if (thread_locals_[thread_id].batch.size() > n_) {
          return OperatorResult::HasMore(std::move(output));
        } else {
          return OperatorResult::Even(std::move(output));
        }
      }
      ARRA_DCHECK(input.has_value());
      thread_locals_[thread_id].batch.insert(thread_locals_[thread_id].batch.end(),
                                             input.value().begin(), input.value().end());
      if (thread_locals_[thread_id].batch.size() >= n_) {
        return f(thread_id, std::nullopt);
      } else {
        return OperatorResult::NeedsMore();
      }
    };

    return f;
  }

  std::optional<PipelineTaskDrain> Drain() override {
    return [&](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].batch.empty()) {
        return OperatorResult::Finished(std::nullopt);
      } else {
        return OperatorResult::Finished(std::move(thread_locals_[thread_id].batch));
      }
    };
  }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  size_t dop_, n_;

 private:
  struct ThreadLocal {
    Batch batch;
  };
  std::vector<ThreadLocal> thread_locals_;
};

TEST(SketchTest, AccumulateThree) {
  {
    size_t dop = 8;
    DistributedMemorySource source(dop, {{1}, {1}, {1}});
    AccumulatePipe pipe(dop, 3);
    MemorySink sink;
    Pipeline pipeline{&source, {&pipe}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(sink.batches_.size(), dop);
    for (size_t i = 0; i < dop; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1, 1, 1}));
    }
  }

  {
    size_t dop = 8;
    DistributedMemorySource source(dop, {{1}, {1}, {1}, {1}, {1}});
    AccumulatePipe pipe(dop, 3);
    MemorySink sink;
    Pipeline pipeline{&source, {&pipe}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(sink.batches_.size(), dop * 2);
    std::sort(sink.batches_.begin(), sink.batches_.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.size() < rhs.size(); });
    for (size_t i = 0; i < dop; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1, 1}));
    }
    for (size_t i = dop; i < dop * 2; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1, 1, 1}));
    }
  }
}

struct BackpressureContext {
  bool backpressure = false;
  bool exit = false;
  size_t source_backpressure = 0, source_non_backpressure = 0;
  size_t pipe_backpressure = 0, pipe_non_backpressure = 0;
};
using BackpressureContexts = std::vector<BackpressureContext>;

class BackpressureSource : public SourceOp {
 public:
  BackpressureSource(BackpressureContexts& ctx) : ctx_(ctx) {}

  PipelineTaskSource Source() override {
    return [&](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      if (ctx_[thread_id].backpressure) {
        ctx_[thread_id].source_backpressure++;
      } else {
        ctx_[thread_id].source_non_backpressure++;
      }

      if (ctx_[thread_id].exit) {
        return OperatorResult::Finished(std::nullopt);
      } else {
        return OperatorResult::HasMore({});
      }
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  BackpressureContexts& ctx_;
};

class BackpressurePipe : public PipeOp {
 public:
  BackpressurePipe(BackpressureContexts& ctx) : ctx_(ctx) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (ctx_[thread_id].backpressure) {
        ctx_[thread_id].pipe_backpressure++;
      } else {
        ctx_[thread_id].pipe_non_backpressure++;
      }
      return OperatorResult::Even(std::move(input.value()));
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  BackpressureContexts& ctx_;
};

class BackpressureSink : public SinkOp {
 public:
  BackpressureSink(size_t dop, BackpressureContexts& ctx, size_t backpressure_start,
                   size_t backpressure_stop, size_t exit)
      : ctx_(ctx),
        backpressure_start_(backpressure_start),
        backpressure_stop_(backpressure_stop),
        exit_(exit) {
    thread_locals_.resize(dop);
  }

  PipelineTaskSink Sink() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      size_t counter = ++thread_locals_[thread_id].counter;
      if (counter >= exit_) {
        ctx_[thread_id].exit = true;
        return OperatorResult::NeedsMore();
      }
      if (counter == backpressure_start_) {
        ctx_[thread_id].backpressure = true;
      }
      if (counter == backpressure_stop_) {
        ctx_[thread_id].backpressure = false;
      }
      if (counter > backpressure_start_ && counter <= backpressure_stop_) {
        ARRA_DCHECK(!input.has_value());
      } else {
        ARRA_DCHECK(input.has_value());
      }
      if (counter >= backpressure_start_ && counter < backpressure_stop_) {
        return OperatorResult::Backpressure();
      }
      return OperatorResult::NeedsMore();
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  BackpressureContexts& ctx_;
  size_t backpressure_start_, backpressure_stop_, exit_;

  struct ThreadLocal {
    size_t counter = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
};

TEST(SketchTest, BasicBackpressure) {
  size_t dop = 8;
  BackpressureContexts ctx(dop);
  BackpressureSource source(ctx);
  BackpressurePipe pipe(ctx);
  BackpressureSink sink(dop, ctx, 100, 200, 300);
  Pipeline pipeline{&source, {&pipe}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  for (const auto& c : ctx) {
    ASSERT_EQ(c.backpressure, false);
    ASSERT_EQ(c.exit, true);
    ASSERT_EQ(c.source_backpressure, 0);
    ASSERT_EQ(c.source_non_backpressure, 201);
    ASSERT_EQ(c.pipe_backpressure, 0);
    ASSERT_EQ(c.pipe_non_backpressure, 200);
  }
}

class ErrorGenerator {
 public:
  ErrorGenerator(size_t trigger) : trigger_(trigger) {}

  template <typename T>
  arrow::Result<T> operator()(T non_error) {
    if (counter_++ == trigger_) {
      return arrow::Status::Invalid(std::to_string(trigger_));
    }
    return non_error;
  }

 private:
  size_t trigger_;

  std::atomic<size_t> counter_ = 0;
};

class ErrorOpWrapper {
 public:
  ErrorOpWrapper(ErrorGenerator* err_gen) : err_gen_(err_gen) {}

 protected:
  template <typename TTask, typename... TArgs>
  auto WrapError(TTask&& task, TArgs&&... args) {
    auto result = task(std::forward<TArgs>(args)...);
    if (err_gen_ == nullptr || !result.ok()) {
      return result;
    }
    return (*err_gen_)(std::move(*result));
  }

  auto WrapTaskGroup(const TaskGroup& task_group) {
    auto task = [this, task = std::get<0>(task_group)](ThreadId thread_id) -> TaskResult {
      return WrapError(task, thread_id);
    };
    auto size = std::get<1>(task_group);
    std::optional<TaskCont> task_cont = std::nullopt;
    if (std::get<2>(task_group).has_value()) {
      task_cont = [this, task_cont = std::get<2>(task_group).value()]() -> TaskResult {
        return WrapError(task_cont);
      };
    }
    return TaskGroup{std::move(task), size, std::move(task_cont)};
  }

  auto WrapTaskGroups(const TaskGroups& task_groups) {
    TaskGroups transformed(task_groups.size());
    std::transform(task_groups.begin(), task_groups.end(), transformed.begin(),
                   [&](const auto& task_group) { return WrapTaskGroup(task_group); });
    return transformed;
  }

 protected:
  ErrorGenerator* err_gen_;
};

class ErrorSource : virtual public SourceOp, public ErrorOpWrapper {
 public:
  ErrorSource(ErrorGenerator* err_gen, SourceOp* source)
      : ErrorOpWrapper(err_gen), source_(source) {}

  PipelineTaskSource Source() override {
    return [&](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      return WrapError(source_->Source(), thread_id);
    };
  }

  TaskGroups Frontend() override {
    auto parent_groups = source_->Frontend();
    return WrapTaskGroups(parent_groups);
  }

  TaskGroups Backend() override {
    auto parent_groups = source_->Backend();
    return WrapTaskGroups(parent_groups);
  }

 private:
  SourceOp* source_;
};

class ErrorPipe : virtual public PipeOp, public ErrorOpWrapper {
 public:
  ErrorPipe(ErrorGenerator* err_gen, PipeOp* pipe)
      : ErrorOpWrapper(err_gen), pipe_(pipe) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      return WrapError(pipe_->Pipe(), thread_id, std::move(input));
    };
  }

  std::optional<PipelineTaskDrain> Drain() override {
    auto drain = pipe_->Drain();
    if (!drain.has_value()) {
      return std::nullopt;
    }
    return [&, drain](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      return WrapError(drain.value(), thread_id);
    };
  }

  std::unique_ptr<SourceOp> Source() override {
    pipe_source_ = pipe_->Source();
    return std::make_unique<ErrorSource>(err_gen_, pipe_source_.get());
  }

 private:
  PipeOp* pipe_;
  std::unique_ptr<SourceOp> pipe_source_;
};

class ErrorSink : virtual public SinkOp, public ErrorOpWrapper {
 public:
  ErrorSink(ErrorGenerator* err_gen, SinkOp* sink)
      : ErrorOpWrapper(err_gen), sink_(sink) {}

  PipelineTaskSink Sink() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      return WrapError(sink_->Sink(), thread_id, std::move(input));
    };
  }

  TaskGroups Frontend() override {
    auto parent_groups = sink_->Frontend();
    return WrapTaskGroups(parent_groups);
  }

  TaskGroups Backend() override {
    auto parent_groups = sink_->Backend();
    return WrapTaskGroups(parent_groups);
  }

 private:
  SinkOp* sink_;
};

class InfiniteSource : public SourceOp {
 public:
  InfiniteSource(Batch batch) : batch_(std::move(batch)) {}

  PipelineTaskSource Source() override {
    return [&](ThreadId) -> arrow::Result<OperatorResult> {
      return OperatorResult::HasMore(batch_);
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  Batch batch_;
};

class BlackHoleSink : public SinkOp {
 public:
  PipelineTaskSink Sink() override {
    return [&](ThreadId, std::optional<Batch>) -> arrow::Result<OperatorResult> {
      return OperatorResult::NeedsMore();
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }
};

TEST(SketchTest, BasicError) {
  {
    size_t dop = 8;
    InfiniteSource source(Batch{});
    IdentityPipe pipe;
    BlackHoleSink sink;
    ErrorGenerator err_gen(42);
    ErrorSource err_source(&err_gen, &source);
    Pipeline pipeline{&err_source, {&pipe}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_NOT_OK(result);
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_EQ(result.status().message(), "42");
  }

  {
    size_t dop = 8;
    InfiniteSource source(Batch{});
    IdentityPipe pipe;
    BlackHoleSink sink;
    ErrorGenerator err_gen(42);
    ErrorPipe err_pipe(&err_gen, &pipe);
    Pipeline pipeline{&source, {&err_pipe}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_NOT_OK(result);
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_EQ(result.status().message(), "42");
  }

  {
    size_t dop = 8;
    InfiniteSource source(Batch{});
    IdentityPipe pipe;
    BlackHoleSink sink;
    ErrorGenerator err_gen(42);
    ErrorSink err_sink(&err_gen, &sink);
    Pipeline pipeline{&source, {&pipe}, &err_sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_NOT_OK(result);
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_EQ(result.status().message(), "42");
  }
}

class SpillThruPipe : public PipeOp {
 public:
  SpillThruPipe(size_t dop) : thread_locals_(dop) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].spilling) {
        ARRA_DCHECK(!input.has_value());
        ARRA_DCHECK(thread_locals_[thread_id].batch.has_value());
        thread_locals_[thread_id].spilling = false;
        return OperatorResult::NeedsMore();
      }
      if (thread_locals_[thread_id].batch.has_value()) {
        ARRA_DCHECK(!input.has_value());
        auto output = std::move(thread_locals_[thread_id].batch.value());
        thread_locals_[thread_id].batch = std::nullopt;
        return OperatorResult::Even(std::move(output));
      }
      ARRA_DCHECK(input.has_value());
      thread_locals_[thread_id].batch = std::move(input.value());
      thread_locals_[thread_id].spilling = true;
      return OperatorResult::Yield();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  struct ThreadLocal {
    std::optional<Batch> batch = std::nullopt;
    bool spilling = false;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class TaskObserver : public FollyFutureDoublePoolScheduler::TaskObserver {
 public:
  TaskObserver(size_t dop)
      : last_results_(dop, TaskStatus::Continue()), io_thread_infos_(dop) {}

  void BeforeTaskRun(const Task& task, TaskId task_id) override {}

  void AfterTaskRun(const Task& task, TaskId task_id, const TaskResult& result) override {
    if (last_results_[task_id].ok() && last_results_[task_id]->IsYield()) {
      io_thread_infos_[task_id].insert(std::make_pair(
          folly::getCurrentThreadID(), folly::getCurrentThreadName().value()));
    }
    last_results_[task_id] = result;
  }

 public:
  std::vector<TaskResult> last_results_;
  std::vector<std::unordered_set<std::pair<ThreadId, std::string>>> io_thread_infos_;
};

TEST(SketchTest, BasicYield) {
  {
    size_t dop = 8;
    MemorySource source({{1}, {1}, {1}});
    SpillThruPipe pipe(dop);
    MemorySink sink;
    Pipeline pipeline{&source, {&pipe}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_OK(result);
    ASSERT_EQ(sink.batches_.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1}));
    }
  }

  {
    size_t dop = 2;
    MemorySource source({{1}, {1}, {1}});
    SpillThruPipe pipe(dop);
    MemorySink sink;
    Pipeline pipeline{&source, {&pipe}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_OK(result);
    ASSERT_EQ(sink.batches_.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1}));
    }
  }

  {
    size_t dop = 8;
    MemorySource source({{1}, {1}, {1}, {1}});
    SpillThruPipe pipe(dop);
    MemorySink sink;
    Pipeline pipeline{&source, {&pipe}, &sink};

    folly::CPUThreadPoolExecutor cpu_executor(4);
    size_t num_io_threads = 1;
    folly::IOThreadPoolExecutor io_executor(num_io_threads);
    TaskObserver observer(dop);
    FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor, &observer);

    Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
    auto result = driver.Run(dop);
    ASSERT_OK(result);
    ASSERT_EQ(sink.batches_.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1}));
    }

    std::unordered_set<std::pair<ThreadId, std::string>> io_thread_info;
    for (size_t i = 0; i < dop; ++i) {
      std::copy(observer.io_thread_infos_[i].begin(), observer.io_thread_infos_[i].end(),
                std::inserter(io_thread_info, io_thread_info.end()));
    }
    ASSERT_EQ(io_thread_info.size(), num_io_threads);
    ASSERT_EQ(io_thread_info.begin()->second.substr(0, 12), "IOThreadPool");
  }
}

class DrainOnlyPipe : public PipeOp {
 public:
  DrainOnlyPipe(size_t dop) : thread_locals_(dop) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      ARRA_DCHECK(input.has_value());
      thread_locals_[thread_id].batches.push_back(std::move(input.value()));
      return OperatorResult::NeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override {
    return [&](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].batches.empty()) {
        return OperatorResult::Finished(std::nullopt);
      }
      auto batch = std::move(thread_locals_[thread_id].batches.front());
      thread_locals_[thread_id].batches.pop_front();
      if (thread_locals_[thread_id].batches.empty()) {
        return OperatorResult::Finished(std::move(batch));
      } else {
        return OperatorResult::HasMore(std::move(batch));
      }
    };
  }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  struct ThreadLocal {
    std::list<Batch> batches;
  };
  std::vector<ThreadLocal> thread_locals_;
};

TEST(SketchTest, Drain) {
  size_t dop = 2;
  MemorySource source({{1}, {1}, {1}, {1}});
  DrainOnlyPipe pipe(dop);
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(1);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_OK(result);
  ASSERT_EQ(sink.batches_.size(), 4);
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(sink.batches_[i], (Batch{1}));
  }
}

TEST(SketchTest, MultiDrain) {
  size_t dop = 2;
  MemorySource source({{1}, {1}, {1}, {1}});
  DrainOnlyPipe pipe_1(dop);
  DrainOnlyPipe pipe_2(dop);
  DrainOnlyPipe pipe_3(dop);
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe_1, &pipe_2, &pipe_3}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(1);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_OK(result);
  ASSERT_EQ(sink.batches_.size(), 4);
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(sink.batches_[i], (Batch{1}));
  }
}

TEST(SketchTest, DrainBackpressure) {}

class DrainErrorPipe : public ErrorPipe, public DrainOnlyPipe {
 public:
  DrainErrorPipe(ErrorGenerator* err_gen, size_t dop)
      : ErrorPipe(err_gen, static_cast<DrainOnlyPipe*>(this)), DrainOnlyPipe(dop) {}

  PipelineTaskPipe Pipe() override { return DrainOnlyPipe::Pipe(); }

 private:
  std::unique_ptr<ErrorGenerator> err_gen_;
};

TEST(SketchTest, DrainError) {
  size_t dop = 2;
  MemorySource source({{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}});
  ErrorGenerator err_gen(7);
  DrainErrorPipe pipe(&err_gen, dop);
  BlackHoleSink sink;
  Pipeline pipeline{&source, {static_cast<ErrorPipe*>(&pipe)}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(1);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(dop);
  ASSERT_NOT_OK(result);
  ASSERT_TRUE(result.status().IsInvalid());
  ASSERT_EQ(result.status().message(), "7");
}

TEST(SketchTest, ErrorAfterBackpressure) {}

TEST(SketchTest, ErrorAfterBackpressure) {}
