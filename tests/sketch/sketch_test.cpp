#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
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
    CANCELLED,
  } code_;

  TaskStatus(Code code) : code_(code) {}

 public:
  bool IsContinue() { return code_ == Code::CONTINUE; }
  bool IsBackpressure() { return code_ == Code::BACKPRESSURE; }
  bool IsYield() { return code_ == Code::YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

 public:
  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Backpressure() { return TaskStatus{Code::BACKPRESSURE}; }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};

using TaskId = size_t;
using ThreadId = size_t;

using Task = std::function<arrow::Result<TaskStatus>(TaskId)>;
using TaskCont = std::function<arrow::Result<TaskStatus>()>;
using TaskGroup = std::tuple<Task, size_t, std::optional<TaskCont>>;
using TaskGroups = std::vector<TaskGroup>;

using Batch = std::vector<int>;

struct OperatorResult {
 private:
  enum class OperatorStatus {
    NEEDS_MORE,
    HAS_EVEN,
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
  bool IsHasEven() { return status_ == OperatorStatus::HAS_EVEN; }
  bool IsHasMore() { return status_ == OperatorStatus::HAS_MORE; }
  bool IsBackpressure() { return status_ == OperatorStatus::BACKPRESSURE; }
  bool IsYield() { return status_ == OperatorStatus::YIELD; }
  bool IsFinished() { return status_ == OperatorStatus::FINISHED; }
  bool IsCancelled() { return status_ == OperatorStatus::CANCELLED; }

  std::optional<Batch>& GetOutput() {
    ARRA_DCHECK(IsHasEven() || IsHasMore() || IsFinished());
    return output_;
  }

 public:
  static OperatorResult NeedsMore() { return OperatorResult(OperatorStatus::NEEDS_MORE); }
  static OperatorResult HasEven(Batch output) {
    return OperatorResult(OperatorStatus::HAS_EVEN, std::move(output));
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

  arrow::Result<TaskStatus> Run(ThreadId thread_id) {
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
      ARRA_DCHECK(result->IsHasMore() || result->IsFinished());
      if (result->GetOutput().has_value()) {
        return Pipe(thread_id, drain_id + 1, std::move(result->GetOutput()));
      }
    }

    return TaskStatus::Finished();
  }

 private:
  arrow::Result<TaskStatus> Pipe(ThreadId thread_id, size_t pipe_id,
                                 std::optional<Batch> input) {
    for (size_t i = pipe_id; i < pipes_.size(); i++) {
      auto result = pipes_[i].first(thread_id, std::move(input));
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      ARRA_DCHECK(result->IsNeedsMore() || result->IsHasEven() || result->IsHasMore());
      if (result->IsHasEven() || result->IsHasMore()) {
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
    if (!result->IsBackpressure()) {
      local_states_[thread_id].backpressure = false;
      return TaskStatus::Continue();
    }
    return TaskStatus::Backpressure();
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
    bool backpressure = false;
  };
  std::vector<ThreadLocalState> local_states_;
  std::atomic_bool cancelled = false, yield = false;
};

template <typename Scheduler>
class Driver {
 public:
  Driver(std::vector<Pipeline> pipelines, Scheduler* scheduler)
      : pipelines_(std::move(pipelines)), scheduler_(scheduler) {}

  arrow::Result<TaskStatus> Run(size_t dop) {
    for (const auto& pipeline : pipelines_) {
      ARRA_ASSIGN_OR_RAISE(
          auto result, RunPipeline(dop, pipeline.source, pipeline.pipes, pipeline.sink));
      ARRA_DCHECK(result.IsFinished());
    }
    return TaskStatus::Finished();
  }

 private:
  arrow::Result<TaskStatus> RunPipeline(size_t dop, SourceOp* source,
                                        const std::vector<PipeOp*>& pipes, SinkOp* sink) {
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

  arrow::Result<TaskStatus> RunPipeline(size_t dop, SourceOp* source,
                                        const std::vector<PipeOp*>& pipes,
                                        size_t pipe_start, SinkOp* sink) {
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
  using ConcreteTask = folly::SemiFuture<arrow::Result<TaskStatus>>;
  using TaskGroupPayload = std::vector<arrow::Result<TaskStatus>>;

 public:
  using TaskGroupHandle =
      std::pair<folly::Future<arrow::Result<TaskStatus>>, TaskGroupPayload>;
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
    handle.first =
        folly::via(&executor_)
            .thenValue([tasks = std::move(tasks)](auto&&) mutable {
              return folly::collectAll(tasks);
            })
            .thenValue([&task_cont](auto&& try_results) -> arrow::Result<TaskStatus> {
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

  arrow::Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& group) {
    return group.first.wait().value();
  }

  arrow::Result<TaskStatus> WaitTaskGroups(TaskGroupsHandle& groups) {
    for (auto& group : groups) {
      ARRA_RETURN_NOT_OK(WaitTaskGroup(group));
    }
    return TaskStatus::Finished();
  }

 private:
  static ConcreteTask MakeTask(const Task& task, TaskId task_id,
                               arrow::Result<TaskStatus>& result) {
    auto pred = [&]() {
      return result.ok() && (result->IsContinue() || result->IsYield());
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

}  // namespace arra::sketch

using namespace arra::sketch;

class OneSource : public SourceOp {
 public:
  PipelineTaskSource Source() override {
    return [&](ThreadId) -> arrow::Result<OperatorResult> {
      if (!done.exchange(true)) {
        return OperatorResult::HasMore({1});
      }
      return OperatorResult::Finished(std::nullopt);
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  std::atomic<bool> done = false;
};

class IdentityPipe : public PipeOp {
 public:
  PipelineTaskPipe Pipe() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        return OperatorResult::HasEven(std::move(input.value()));
      }
      return OperatorResult::NeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }
};

class TimesNInOnePipe : public PipeOp {
 public:
  TimesNInOnePipe(size_t n) : n_(n) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        auto& batch = input.value();
        Batch output(batch.size() * n_);
        for (size_t i = 0; i < n_; ++i) {
          std::copy(batch.begin(), batch.end(), output.begin() + i * batch.size());
        }
        return OperatorResult::HasEven(std::move(output));
      }
      return OperatorResult::NeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  size_t n_;
};

class TimesNInNPipe : public PipeOp {
 public:
  TimesNInNPipe(size_t n, size_t dop) : n_(n), dop_(dop) { thread_locals_.resize(dop_); }

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
      auto batch = thread_locals_[thread_id].batches.back();
      thread_locals_[thread_id].batches.pop_back();
      if (thread_locals_[thread_id].batches.empty()) {
        return OperatorResult::HasEven(std::move(batch));
      } else {
        return OperatorResult::HasMore(std::move(batch));
      }
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  size_t n_, dop_;

 private:
  struct ThreadLocal {
    std::vector<Batch> batches;
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

TEST(SketchTest, OneToOne) {
  OneSource source;
  IdentityPipe pipe;
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(8);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 1);
  ASSERT_EQ(sink.batches_[0], Batch{1});
}

TEST(SketchTest, OneToThreeInOne) {
  OneSource source;
  TimesNInOnePipe pipe(3);
  MemorySink sink;
  Pipeline pipeline{&source, {&pipe}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
  auto result = driver.Run(8);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 1);
  ASSERT_EQ(sink.batches_[0], (Batch{1, 1, 1}));
}

TEST(SketchTest, OneToThreeInThree) {
  size_t dop = 8;
  OneSource source;
  TimesNInNPipe pipe(3, dop);
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
