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
  enum class Code {
    SOURCE_NOT_READY,
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    SINK_BACKPRESSURE,
    PIPE_YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  std::optional<Batch> output_;

  OperatorResult(Code code, std::optional<Batch> output = std::nullopt)
      : code_(code), output_(std::move(output)) {}

 public:
  bool IsSourceNotReady() { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsSinkBackpressure() { return code_ == Code::SINK_BACKPRESSURE; }
  bool IsPipeYield() { return code_ == Code::PIPE_YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetOutput() {
    ARRA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return output_;
  }

 public:
  static OperatorResult SourceNotReady() {
    return OperatorResult(Code::SOURCE_NOT_READY);
  }
  static OperatorResult PipeSinkNeedsMore() {
    return OperatorResult(Code::PIPE_SINK_NEEDS_MORE);
  }
  static OperatorResult PipeEven(Batch output) {
    return OperatorResult(Code::PIPE_EVEN, std::move(output));
  }
  static OperatorResult SourcePipeHasMore(Batch output) {
    return OperatorResult{Code::SOURCE_PIPE_HAS_MORE, std::move(output)};
  }
  static OperatorResult SinkBackpressure() {
    return OperatorResult{Code::SINK_BACKPRESSURE};
  }
  static OperatorResult PipeYield() { return OperatorResult{Code::PIPE_YIELD}; }
  static OperatorResult Finished(std::optional<Batch> output) {
    return OperatorResult{Code::FINISHED, std::move(output)};
  }
  static OperatorResult Cancelled() { return OperatorResult{Code::CANCELLED}; }
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

struct PipelinePlex {
  SourceOp* source;
  std::vector<PipeOp*> pipes;
};

using Pipeline = std::pair<std::vector<PipelinePlex>, SinkOp*>;

class PipelinePlexTask {
 public:
  PipelinePlexTask(
      size_t dop, PipelineTaskSource source,
      std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>> pipes,
      PipelineTaskSink sink)
      : dop_(dop),
        source_(std::move(source)),
        pipes_(std::move(pipes)),
        sink_(std::move(sink)),
        local_states_(dop) {
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

  arrow::Result<OperatorResult> Run(ThreadId thread_id) {
    if (cancelled) {
      return OperatorResult::Cancelled();
    }

    if (local_states_[thread_id].backpressure) {
      auto result = sink_(thread_id, std::nullopt);
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      ARRA_DCHECK(result->IsPipeSinkNeedsMore() || result->IsSinkBackpressure());
      if (!result->IsSinkBackpressure()) {
        local_states_[thread_id].backpressure = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      return OperatorResult::SinkBackpressure();
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
      if (result->IsSourceNotReady()) {
        return OperatorResult::SourceNotReady();
      } else if (result->IsFinished()) {
        local_states_[thread_id].source_done = true;
        if (result->GetOutput().has_value()) {
          return Pipe(thread_id, 0, std::move(result->GetOutput()));
        }
      } else {
        ARRA_DCHECK(result->IsSourcePipeHasMore());
        ARRA_DCHECK(result->GetOutput().has_value());
        return Pipe(thread_id, 0, std::move(result->GetOutput()));
      }
    }

    if (local_states_[thread_id].draining >= local_states_[thread_id].drains.size()) {
      return OperatorResult::Finished(std::nullopt);
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
        ARRA_DCHECK(result->IsPipeSinkNeedsMore());
        local_states_[thread_id].yield = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (result->IsPipeYield()) {
        ARRA_DCHECK(!local_states_[thread_id].yield);
        local_states_[thread_id].yield = true;
        return OperatorResult::PipeYield();
      }
      ARRA_DCHECK(result->IsSourcePipeHasMore() || result->IsFinished());
      if (result->GetOutput().has_value()) {
        if (result->IsFinished()) {
          ++local_states_[thread_id].draining;
        }
        return Pipe(thread_id, drain_id + 1, std::move(result->GetOutput()));
      }
    }

    return OperatorResult::Finished(std::nullopt);
  }

 private:
  arrow::Result<OperatorResult> Pipe(ThreadId thread_id, size_t pipe_id,
                                     std::optional<Batch> input) {
    for (size_t i = pipe_id; i < pipes_.size(); i++) {
      auto result = pipes_[i].first(thread_id, std::move(input));
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (local_states_[thread_id].yield) {
        ARRA_DCHECK(result->IsPipeSinkNeedsMore());
        local_states_[thread_id].pipe_stack.push(i);
        local_states_[thread_id].yield = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (result->IsPipeYield()) {
        ARRA_DCHECK(!local_states_[thread_id].yield);
        local_states_[thread_id].pipe_stack.push(i);
        local_states_[thread_id].yield = true;
        return OperatorResult::PipeYield();
      }
      ARRA_DCHECK(result->IsPipeSinkNeedsMore() || result->IsPipeEven() ||
                  result->IsSourcePipeHasMore());
      if (result->IsPipeEven() || result->IsSourcePipeHasMore()) {
        if (result->IsSourcePipeHasMore()) {
          local_states_[thread_id].pipe_stack.push(i);
        }
        ARRA_DCHECK(result->GetOutput().has_value());
        input = std::move(result->GetOutput());
      } else {
        return OperatorResult::PipeSinkNeedsMore();
      }
    }

    auto result = sink_(thread_id, std::move(input));
    if (!result.ok()) {
      cancelled = true;
      return result.status();
    }
    ARRA_DCHECK(result->IsPipeSinkNeedsMore() || result->IsSinkBackpressure());
    if (result->IsSinkBackpressure()) {
      local_states_[thread_id].backpressure = true;
      return OperatorResult::SinkBackpressure();
    }
    return OperatorResult::PipeSinkNeedsMore();
  }

 private:
  size_t dop_;
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

struct PipelineMultiplexTask {
  PipelineMultiplexTask(std::vector<std::unique_ptr<PipelinePlexTask>> tasks)
      : tasks_(std::move(tasks)) {}

  arrow::Result<OperatorResult> Run(ThreadId thread_id) {
    for (auto& task : tasks_) {
      ARRA_ASSIGN_OR_RAISE(auto result, task->Run(thread_id));
      if (result.IsFinished()) {
        ARRA_DCHECK(!result.GetOutput().has_value());
      }
      if (!result.IsFinished() && !result.IsSourceNotReady()) {
        return result;
      }
    }
    return OperatorResult::Finished(std::nullopt);
  }

  std::vector<std::unique_ptr<PipelinePlexTask>> tasks_;
};

using PipelineStage = std::pair<std::vector<std::unique_ptr<SourceOp>>,
                                std::unique_ptr<PipelineMultiplexTask>>;
using PipelineStages = std::vector<PipelineStage>;

class PipelineStagesBuilder {
 public:
  PipelineStagesBuilder(size_t dop, const Pipeline& pipeline)
      : dop_(dop), pipeline_(pipeline) {}

  PipelineStages Build() && {
    BuildTopology();
    SortTopology();
    return BuildStages();
  }

 private:
  void BuildTopology() {
    std::unordered_map<PipeOp*, SourceOp*> pipe_source_map;
    auto sink = pipeline_.second;
    for (auto& plex : pipeline_.first) {
      size_t stage = 0;
      topology_.emplace(plex.source, std::pair<size_t, PipelinePlex>{stage++, plex});
      for (size_t i = 0; i < plex.pipes.size(); ++i) {
        auto pipe = plex.pipes[i];
        if (pipe_source_map.count(pipe) == 0) {
          if (auto pipe_source_up = pipe->Source(); pipe_source_up != nullptr) {
            auto pipe_source = pipe_source_up.get();
            pipe_source_map.emplace(pipe, pipe_source);
            PipelinePlex new_plex{
                pipe_source,
                std::vector<PipeOp*>(plex.pipes.begin() + i + 1, plex.pipes.end())};
            topology_.emplace(pipe_source, std::pair<size_t, PipelinePlex>{
                                               stage++, std::move(new_plex)});
            pipe_sources_keepalive_.emplace(pipe_source, std::move(pipe_source_up));
          }
        } else {
          auto pipe_source = pipe_source_map[pipe];
          if (topology_[pipe_source].first < stage) {
            topology_[pipe_source].first = stage++;
          }
        }
      }
    }
  }

  void SortTopology() {
    for (auto& [source, stage_info] : topology_) {
      if (pipe_sources_keepalive_.count(source) > 0) {
        stages_[stage_info.first].first.push_back(
            std::move(pipe_sources_keepalive_[source]));
      }
      stages_[stage_info.first].second.push_back(std::move(stage_info.second));
    }
  }

  PipelineStages BuildStages() {
    PipelineStages task_stages;
    for (auto& [stage, stage_info] : stages_) {
      auto sources_keepalive = std::move(stage_info.first);
      std::vector<std::unique_ptr<PipelinePlexTask>> plex_tasks;
      for (auto& plex : stage_info.second) {
        std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>> pipes(
            plex.pipes.size());
        std::transform(
            plex.pipes.begin(), plex.pipes.end(), pipes.begin(),
            [&](auto* pipe) { return std::make_pair(pipe->Pipe(), pipe->Drain()); });
        plex_tasks.push_back(std::make_unique<PipelinePlexTask>(
            dop_, plex.source->Source(), std::move(pipes), pipeline_.second->Sink()));
      }
      task_stages.emplace_back(
          std::move(sources_keepalive),
          std::make_unique<PipelineMultiplexTask>(std::move(plex_tasks)));
    }
    return task_stages;
  }

  size_t dop_;
  const Pipeline& pipeline_;

  std::unordered_map<SourceOp*, std::pair<size_t, PipelinePlex>> topology_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> pipe_sources_keepalive_;
  std::map<size_t,
           std::pair<std::vector<std::unique_ptr<SourceOp>>, std::vector<PipelinePlex>>>
      stages_;
};

template <typename Scheduler>
class Driver {
 public:
  Driver(Scheduler* scheduler) : scheduler_(scheduler) {}

  // TODO: Inter-pipeline dependencies.
  TaskResult Run(size_t dop, std::vector<Pipeline> pipelines) {
    for (const auto& pipeline : pipelines) {
      ARRA_ASSIGN_OR_RAISE(auto result, RunPipeline(dop, pipeline));
      ARRA_DCHECK(result.IsFinished());
    }
    return TaskStatus::Finished();
  }

 private:
  TaskResult RunPipeline(size_t dop, const Pipeline& pipeline) {
    auto stages = PipelineStagesBuilder(dop, pipeline).Build();
    auto sink = pipeline.second;

    // TODO: Backend should be waited even error happens.
    auto sink_be = sink->Backend();
    auto sink_be_handle = scheduler_->ScheduleTaskGroups(sink_be);

    for (auto& stage : stages) {
      ARRA_ASSIGN_OR_RAISE(auto result, RunStage(dop, stage));
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

  TaskResult RunStage(size_t dop, PipelineStage& stage) {
    // TODO: Backend should be waited even error happens.
    std::vector<typename Scheduler::TaskGroupsHandle> source_be_handles;
    for (auto& source : stage.first) {
      auto source_be = source->Backend();
      source_be_handles.push_back(scheduler_->ScheduleTaskGroups(source_be));
    }

    for (auto& source : stage.first) {
      auto source_fe = source->Frontend();
      auto source_fe_handle = scheduler_->ScheduleTaskGroups(source_fe);
      ARRA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroups(source_fe_handle));
      ARRA_DCHECK(result.IsFinished());
    }

    TaskGroup pipeline_task_group{[&](ThreadId thread_id) -> TaskResult {
                                    ARRA_ASSIGN_OR_RAISE(auto result,
                                                         stage.second->Run(thread_id));
                                    if (result.IsSinkBackpressure()) {
                                      return TaskStatus::Backpressure();
                                    }
                                    if (result.IsPipeYield()) {
                                      return TaskStatus::Yield();
                                    }
                                    if (result.IsFinished()) {
                                      return TaskStatus::Finished();
                                    }
                                    if (result.IsCancelled()) {
                                      return TaskStatus::Cancelled();
                                    }
                                    return TaskStatus::Continue();
                                  },
                                  dop, std::nullopt};
    auto pipeline_task_group_handle = scheduler_->ScheduleTaskGroup(pipeline_task_group);
    ARRA_ASSIGN_OR_RAISE(auto result,
                         scheduler_->WaitTaskGroup(pipeline_task_group_handle));
    ARRA_DCHECK(result.IsFinished());

    for (auto& source_be_handle : source_be_handles) {
      ARRA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroups(source_be_handle));
      ARRA_DCHECK(result.IsFinished());
    }

    return TaskStatus::Finished();
  }

 private:
  Scheduler* scheduler_;
};

class FollyFutureScheduler {
 private:
  using ConcreteTask = folly::SemiFuture<TaskResult>;
  using TaskGroupPayload = std::vector<TaskResult>;
  using ConcreteTaskGroup = std::pair<folly::SemiFuture<TaskResult>, TaskGroupPayload>;
  using ConcoretTaskGroups = ConcreteTaskGroup;

 public:
  using TaskGroupHandle = std::pair<folly::Future<TaskResult>, TaskGroupPayload>;
  using TaskGroupsHandle = TaskGroupHandle;

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

// TODO: Maybe a C++20 coroutine-based scheduler.

class InfiniteSource : public SourceOp {
 public:
  InfiniteSource(Batch batch) : batch_(std::move(batch)) {}

  PipelineTaskSource Source() override {
    return [&](ThreadId) -> arrow::Result<OperatorResult> {
      return OperatorResult::SourcePipeHasMore(batch_);
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  Batch batch_;
};

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
        return OperatorResult::SourcePipeHasMore(std::move(output));
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
        return OperatorResult::SourcePipeHasMore(std::move(output));
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

class BlackHoleSink : public SinkOp {
 public:
  PipelineTaskSink Sink() override {
    return [&](ThreadId, std::optional<Batch>) -> arrow::Result<OperatorResult> {
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }
};

class MemorySink : public SinkOp {
 public:
  PipelineTaskSink Sink() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        std::lock_guard<std::mutex> lock(mutex_);
        batches_.push_back(std::move(input.value()));
      }
      return OperatorResult::PipeSinkNeedsMore();
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
        return OperatorResult::PipeEven(std::move(input.value()));
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }
};

class IdentityWithAnotherSourcePipe : public IdentityPipe {
 public:
  IdentityWithAnotherSourcePipe(std::unique_ptr<SourceOp> source)
      : source_(std::move(source)) {}

  std::unique_ptr<SourceOp> Source() override { return std::move(source_); }

  std::unique_ptr<SourceOp> source_;
};

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
        return OperatorResult::PipeEven(std::move(output));
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  size_t n_;
};

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
        return OperatorResult::PipeEven(std::move(output));
      } else {
        return OperatorResult::SourcePipeHasMore(std::move(output));
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

class AccumulatePipe : public PipeOp {
 public:
  AccumulatePipe(size_t dop, size_t n) : dop_(dop), n_(n) { thread_locals_.resize(dop_); }

  arrow::Result<OperatorResult> Pipe(ThreadId thread_id, std::optional<Batch> input) {
    if (thread_locals_[thread_id].batch.size() >= n_) {
      ARRA_DCHECK(!input.has_value());
      Batch output(thread_locals_[thread_id].batch.begin(),
                   thread_locals_[thread_id].batch.begin() + n_);
      thread_locals_[thread_id].batch =
          Batch(thread_locals_[thread_id].batch.begin() + n_,
                thread_locals_[thread_id].batch.end());
      if (thread_locals_[thread_id].batch.size() > n_) {
        return OperatorResult::SourcePipeHasMore(std::move(output));
      } else {
        return OperatorResult::PipeEven(std::move(output));
      }
    }
    ARRA_DCHECK(input.has_value());
    thread_locals_[thread_id].batch.insert(thread_locals_[thread_id].batch.end(),
                                           input.value().begin(), input.value().end());
    if (thread_locals_[thread_id].batch.size() >= n_) {
      return Pipe(thread_id, std::nullopt);
    } else {
      return OperatorResult::PipeSinkNeedsMore();
    }
  }

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      return Pipe(thread_id, std::move(input));
    };
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

struct BackpressureContext {
  bool backpressure = false;
  bool exit = false;
  size_t source_backpressure = 0, source_non_backpressure = 0;
  size_t pipe_backpressure = 0, pipe_non_backpressure = 0;
};
using BackpressureContexts = std::vector<BackpressureContext>;

class BackpressureDelegateSource : public SourceOp {
 public:
  BackpressureDelegateSource(BackpressureContexts& ctx, SourceOp* source)
      : ctx_(ctx), source_(source) {}

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
        return source_->Source()(thread_id);
      }
    };
  }

  TaskGroups Frontend() override { return source_->Frontend(); }

  TaskGroups Backend() override { return source_->Backend(); }

 private:
  BackpressureContexts& ctx_;
  SourceOp* source_;
};

class BackpressureDelegatePipe : public PipeOp {
 public:
  BackpressureDelegatePipe(BackpressureContexts& ctx, PipeOp* pipe)
      : ctx_(ctx), pipe_(pipe) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (ctx_[thread_id].backpressure) {
        ctx_[thread_id].pipe_backpressure++;
      } else {
        ctx_[thread_id].pipe_non_backpressure++;
      }
      return pipe_->Pipe()(thread_id, std::move(input.value()));
    };
  }

  std::optional<PipelineTaskDrain> Drain() override {
    auto drain = pipe_->Drain();
    if (!drain.has_value()) {
      return std::nullopt;
    }
    return [&, drain](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      if (ctx_[thread_id].backpressure) {
        ctx_[thread_id].pipe_backpressure++;
      } else {
        ctx_[thread_id].pipe_non_backpressure++;
      }
      return drain.value()(thread_id);
    };
  }

  std::unique_ptr<SourceOp> Source() override { return pipe_->Source(); }

 private:
  BackpressureContexts& ctx_;
  PipeOp* pipe_;
};

class BackpressureDelegateSink : public SinkOp {
 public:
  BackpressureDelegateSink(size_t dop, BackpressureContexts& ctx,
                           size_t backpressure_start, size_t backpressure_stop,
                           size_t exit, SinkOp* sink)
      : ctx_(ctx),
        backpressure_start_(backpressure_start),
        backpressure_stop_(backpressure_stop),
        exit_(exit),
        sink_(sink) {
    thread_locals_.resize(dop);
  }

  PipelineTaskSink Sink() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      size_t counter = ++thread_locals_[thread_id].counter;
      if (counter >= exit_) {
        ctx_[thread_id].exit = true;
        return sink_->Sink()(thread_id, std::move(input.value()));
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
        ARRA_RETURN_NOT_OK(sink_->Sink()(thread_id, std::move(input.value())));
      }
      if (counter >= backpressure_start_ && counter < backpressure_stop_) {
        return OperatorResult::SinkBackpressure();
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override { return sink_->Frontend(); }

  TaskGroups Backend() override { return sink_->Backend(); }

 private:
  BackpressureContexts& ctx_;
  size_t backpressure_start_, backpressure_stop_, exit_;
  SinkOp* sink_;

  struct ThreadLocal {
    size_t counter = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
};

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

class ErrorDelegateSource : virtual public SourceOp, public ErrorOpWrapper {
 public:
  ErrorDelegateSource(ErrorGenerator* err_gen, SourceOp* source)
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

class ErrorDelegatePipe : virtual public PipeOp, public ErrorOpWrapper {
 public:
  ErrorDelegatePipe(ErrorGenerator* err_gen, PipeOp* pipe)
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
    return std::make_unique<ErrorDelegateSource>(err_gen_, pipe_source_.get());
  }

 private:
  PipeOp* pipe_;
  std::unique_ptr<SourceOp> pipe_source_;
};

class ErrorDelegateSink : virtual public SinkOp, public ErrorOpWrapper {
 public:
  ErrorDelegateSink(ErrorGenerator* err_gen, SinkOp* sink)
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

class SpillDelegatePipe : public PipeOp {
 public:
  SpillDelegatePipe(size_t dop, PipeOp* pipe) : thread_locals_(dop), pipe_(pipe) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].spilling) {
        ARRA_DCHECK(!input.has_value());
        ARRA_DCHECK(thread_locals_[thread_id].result.has_value());
        thread_locals_[thread_id].spilling = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (thread_locals_[thread_id].result.has_value()) {
        ARRA_DCHECK(!input.has_value());
        ARRA_DCHECK(!thread_locals_[thread_id].spilling);
        auto result = std::move(thread_locals_[thread_id].result.value());
        thread_locals_[thread_id].result = std::nullopt;
        return result;
      }
      ARRA_DCHECK(input.has_value());
      ARRA_ASSIGN_OR_RAISE(thread_locals_[thread_id].result,
                           pipe_->Pipe()(thread_id, std::move(input)));
      thread_locals_[thread_id].spilling = true;
      return OperatorResult::PipeYield();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return pipe_->Drain(); }

  std::unique_ptr<SourceOp> Source() override { return pipe_->Source(); }

 private:
  PipeOp* pipe_;

  struct ThreadLocal {
    std::optional<arrow::Result<OperatorResult>> result = std::nullopt;
    bool spilling = false;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class DrainOnlyPipe : public PipeOp {
 public:
  DrainOnlyPipe(size_t dop) : thread_locals_(dop) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      ARRA_DCHECK(input.has_value());
      thread_locals_[thread_id].batches.push_back(std::move(input.value()));
      return OperatorResult::PipeSinkNeedsMore();
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
        return OperatorResult::SourcePipeHasMore(std::move(batch));
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

class DrainErrorPipe : public ErrorDelegatePipe, public DrainOnlyPipe {
 public:
  DrainErrorPipe(ErrorGenerator* err_gen, size_t dop)
      : ErrorDelegatePipe(err_gen, static_cast<DrainOnlyPipe*>(this)),
        DrainOnlyPipe(dop) {}

  PipelineTaskPipe Pipe() override { return DrainOnlyPipe::Pipe(); }

 private:
  std::unique_ptr<ErrorGenerator> err_gen_;
};

class DrainSpillDelegatePipe : public PipeOp {
 public:
  DrainSpillDelegatePipe(size_t dop, PipeOp* pipe) : thread_locals_(dop), pipe_(pipe) {}

  PipelineTaskPipe Pipe() override { return pipe_->Pipe(); }

  std::optional<PipelineTaskDrain> Drain() override {
    auto drain = pipe_->Drain();
    if (!drain.has_value()) {
      return std::nullopt;
    }
    return [&, drain](ThreadId thread_id) -> arrow::Result<OperatorResult> {
      if (thread_locals_[thread_id].spilling) {
        ARRA_DCHECK(thread_locals_[thread_id].result.has_value());
        thread_locals_[thread_id].spilling = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (thread_locals_[thread_id].result.has_value()) {
        ARRA_DCHECK(!thread_locals_[thread_id].spilling);
        auto result = std::move(thread_locals_[thread_id].result.value());
        thread_locals_[thread_id].result = std::nullopt;
        return result;
      }
      ARRA_ASSIGN_OR_RAISE(thread_locals_[thread_id].result, drain.value()(thread_id));
      thread_locals_[thread_id].spilling = true;
      return OperatorResult::PipeYield();
    };
  }

  std::unique_ptr<SourceOp> Source() override { return pipe_->Source(); }

 private:
  PipeOp* pipe_;

  struct ThreadLocal {
    std::optional<arrow::Result<OperatorResult>> result = std::nullopt;
    bool spilling = false;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class ProjectPipe : public PipeOp {
 public:
  ProjectPipe(std::function<Batch(Batch&)> expr) : expr_(expr) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        return OperatorResult::PipeEven(expr_(input.value()));
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override { return nullptr; }

 private:
  std::function<Batch(Batch&)> expr_;
};

class FibonacciSource : public SourceOp {
 public:
  PipelineTaskSource Source() override {
    return [&](ThreadId) -> arrow::Result<OperatorResult> {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!done_) {
        done_ = true;
        return OperatorResult::Finished(Batch{1});
      }
      return OperatorResult::Finished(std::nullopt);
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override { return {}; }

 private:
  std::mutex mutex_;
  bool done_ = false;
};

class FibonacciPipe : public PipeOp {
 public:
  PipelineTaskPipe Pipe() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      ARRA_DCHECK(input.has_value() && input.value().size() == 1);
      std::lock_guard<std::mutex> lock(mutex_);
      if (!first_.has_value()) {
        first_ = input.value()[0];
        return OperatorResult::PipeEven(std::move(input.value()));
      }
      if (!second_.has_value()) {
        second_ = input.value()[0];
        return OperatorResult::PipeEven(std::move(input.value()));
      }
      first_ = std::move(second_);
      second_ = input.value()[0];
      return OperatorResult::PipeEven(std::move(input.value()));
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

 private:
  class InternalSource : public SourceOp {
   public:
    InternalSource(std::optional<int>& first, std::optional<int>& second)
        : done_(false), first_(first), second_(second) {}

    PipelineTaskSource Source() override {
      return [&](ThreadId) -> arrow::Result<OperatorResult> {
        std::lock_guard<std::mutex> lock(mutex_);
        ARRA_DCHECK(first_.has_value() && second_.has_value());
        if (!done_) {
          done_ = true;
          return OperatorResult::Finished(Batch{first_.value() + second_.value()});
        }
        return OperatorResult::Finished(std::nullopt);
      };
    }

    TaskGroups Frontend() override { return {}; }

    TaskGroups Backend() override { return {}; }

   private:
    std::mutex mutex_;
    bool done_;
    std::optional<int>&first_, &second_;
  };

 public:
  std::unique_ptr<SourceOp> Source() override {
    return std::make_unique<InternalSource>(first_, second_);
  }

 private:
  std::mutex mutex_;
  std::optional<int> first_ = std::nullopt, second_ = std::nullopt;
};

class PowerPipe : public PipeOp {
 public:
  PowerPipe(size_t n) : n_(n) {}

  PipelineTaskPipe Pipe() override {
    return [&](ThreadId, std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        for (size_t i = 1; i < n_; ++i) {
          std::lock_guard<std::mutex> lock(mutex_);
          batches_.push_back(input.value());
        }
        return OperatorResult::PipeEven(std::move(input.value()));
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  std::optional<PipelineTaskDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> Source() override {
    return std::make_unique<MemorySource>(std::move(batches_));
  }

 private:
  size_t n_;

  std::mutex mutex_;
  std::list<Batch> batches_;
};

class SortSink : public SinkOp {
 public:
  SortSink(size_t dop) : dop_(dop), thread_locals_(dop) {}

  PipelineTaskSink Sink() override {
    return [&](ThreadId thread_id,
               std::optional<Batch> input) -> arrow::Result<OperatorResult> {
      if (input.has_value()) {
        std::sort(input.value().begin(), input.value().end());
        Batch merged(input.value().size() + thread_locals_[thread_id].sorted.size());
        std::merge(input.value().begin(), input.value().end(),
                   thread_locals_[thread_id].sorted.begin(),
                   thread_locals_[thread_id].sorted.end(), merged.begin());
        thread_locals_[thread_id].sorted = std::move(merged);
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override {
    TaskGroups task_groups;
    size_t num_payloads = dop_;
    while (num_payloads > 2) {
      size_t num_tasks = arrow::bit_util::CeilDiv(num_payloads, 2);
      task_groups.push_back({[&, num_payloads](ThreadId thread_id) -> TaskResult {
                               return Merge(thread_id, num_payloads);
                             },
                             num_tasks, std::nullopt});
      num_payloads = num_tasks;
    }
    task_groups.push_back({[&, num_payloads](ThreadId thread_id) -> TaskResult {
                             ARRA_DCHECK(thread_id == 0);
                             return Merge(thread_id, num_payloads);
                           },
                           1,
                           [&]() -> TaskResult {
                             sorted = std::move(thread_locals_[0].sorted);
                             return TaskStatus::Finished();
                           }});
    return task_groups;
  }

  TaskGroups Backend() override { return {}; }

 private:
  TaskResult Merge(ThreadId thread_id, size_t num_payloads) {
    size_t first = thread_id,
           second = thread_id + arrow::bit_util::CeilDiv(num_payloads, 2);
    if (second >= num_payloads) {
      std::stringstream ss;
      ss << "Bypass: first: " << first << ", second: " << second
         << ", num_payloads: " << num_payloads << std::endl;
      std::cout << ss.str();
      return TaskStatus::Finished();
    }
    std::stringstream ss;
    ss << "Merge: first: " << first << ", second: " << second
       << ", num_payloads: " << num_payloads << std::endl;
    std::cout << ss.str();
    Batch merged(thread_locals_[first].sorted.size() +
                 thread_locals_[second].sorted.size());
    std::merge(thread_locals_[first].sorted.begin(), thread_locals_[first].sorted.end(),
               thread_locals_[second].sorted.begin(), thread_locals_[second].sorted.end(),
               merged.begin());
    thread_locals_[first].sorted = std::move(merged);
    return TaskStatus::Finished();
  }

 private:
  size_t dop_;

  struct ThreadLocal {
    Batch sorted;
  };
  std::vector<ThreadLocal> thread_locals_;

 public:
  Batch sorted;
};

}  // namespace arra::sketch

using namespace arra::sketch;

TEST(PipelineTest, EmptyPipeline) {
  size_t dop = 8;
  BlackHoleSink sink;
  Pipeline pipeline{{}, &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();
  ASSERT_EQ(stages.size(), 0);
}

TEST(PipelineTest, SinglePlexPipeline) {
  size_t dop = 8;
  InfiniteSource source({});
  IdentityPipe pipe;
  BlackHoleSink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();
  ASSERT_EQ(stages.size(), 1);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 1);
}

TEST(PipelineTest, DoublePlexPipeline) {
  size_t dop = 8;
  InfiniteSource source_1({}), source_2({});
  IdentityPipe pipe;
  BlackHoleSink sink;
  Pipeline pipeline{{{&source_1, {&pipe}}, {&source_2, {&pipe}}}, &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();
  ASSERT_EQ(stages.size(), 1);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 2);
}

TEST(PipelineTest, DoubleStagePipeline) {
  size_t dop = 8;
  InfiniteSource source({});
  IdentityWithAnotherSourcePipe pipe(std::make_unique<InfiniteSource>(Batch{}));
  BlackHoleSink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();
  ASSERT_EQ(stages.size(), 2);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_EQ(stages[1].first.size(), 1);
  ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[0].get()), nullptr);
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 1);
  ASSERT_NE(stages[1].second, nullptr);
  ASSERT_EQ(stages[1].second->tasks_.size(), 1);
}

TEST(PipelineTest, DoubleStageDoublePlexPipeline) {
  size_t dop = 8;
  InfiniteSource source_1({}), source_2({});
  IdentityWithAnotherSourcePipe pipe_1(std::make_unique<InfiniteSource>(Batch{})),
      pipe_2(std::make_unique<MemorySource>(std::list<Batch>()));
  auto pipe_source_1 = pipe_1.source_.get(), pipe_source_2 = pipe_2.source_.get();
  BlackHoleSink sink;
  Pipeline pipeline{{{&source_1, {&pipe_1}}, {&source_2, {&pipe_2}}}, &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();
  ASSERT_EQ(stages.size(), 2);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_EQ(stages[1].first.size(), 2);
  if (stages[1].first[0].get() == pipe_source_1) {
    ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[0].get()), nullptr);
    ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].first[1].get()), nullptr);
    ASSERT_EQ(stages[1].first[1].get(), pipe_source_2);
  } else {
    ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].first[0].get()), nullptr);
    ASSERT_EQ(stages[1].first[0].get(), pipe_source_2);
    ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[1].get()), nullptr);
    ASSERT_EQ(stages[1].first[1].get(), pipe_source_1);
  }
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 2);
  ASSERT_NE(stages[1].second, nullptr);
  ASSERT_EQ(stages[1].second->tasks_.size(), 2);
}

TEST(PipelineTest, TrippleStagePipeline) {
  size_t dop = 8;
  InfiniteSource source_1({}), source_2({});
  IdentityWithAnotherSourcePipe pipe_1(std::make_unique<InfiniteSource>(Batch{})),
      pipe_2(std::make_unique<MemorySource>(std::list<Batch>())),
      pipe_3(std::make_unique<DistributedMemorySource>(dop, std::list<Batch>()));
  auto pipe_source_1 = pipe_1.source_.get(), pipe_source_2 = pipe_2.source_.get(),
       pipe_source_3 = pipe_3.source_.get();
  BlackHoleSink sink;
  Pipeline pipeline{{{&source_1, {&pipe_1, &pipe_3}}, {&source_2, {&pipe_2, &pipe_3}}},
                    &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();
  ASSERT_EQ(stages.size(), 3);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_EQ(stages[1].first.size(), 2);
  ASSERT_EQ(stages[2].first.size(), 1);
  if (stages[1].first[0].get() == pipe_source_1) {
    ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[0].get()), nullptr);
    ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].first[1].get()), nullptr);
    ASSERT_EQ(stages[1].first[1].get(), pipe_source_2);
  } else {
    ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].first[0].get()), nullptr);
    ASSERT_EQ(stages[1].first[0].get(), pipe_source_2);
    ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[1].get()), nullptr);
    ASSERT_EQ(stages[1].first[1].get(), pipe_source_1);
  }
  ASSERT_NE(dynamic_cast<DistributedMemorySource*>(stages[2].first[0].get()), nullptr);
  ASSERT_EQ(stages[2].first[0].get(), pipe_source_3);
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 2);
  ASSERT_NE(stages[1].second, nullptr);
  ASSERT_EQ(stages[1].second->tasks_.size(), 2);
  ASSERT_NE(stages[2].second, nullptr);
  ASSERT_EQ(stages[2].second->tasks_.size(), 1);
}

TEST(PipelineTest, OddQuadroStagePipeline) {
  size_t dop = 8;
  InfiniteSource source_1({}), source_2({}), source_3({}), source_4({});
  IdentityWithAnotherSourcePipe pipe_1_1(std::make_unique<InfiniteSource>(Batch{})),
      pipe_1_2(std::make_unique<InfiniteSource>(Batch{}));
  IdentityWithAnotherSourcePipe pipe_2_1(
      std::make_unique<MemorySource>(std::list<Batch>()));
  IdentityWithAnotherSourcePipe pipe_3_1(
      std::make_unique<DistributedMemorySource>(dop, std::list<Batch>()));
  BlackHoleSink sink;
  Pipeline pipeline{{{&source_1, {&pipe_1_1, &pipe_2_1, &pipe_3_1}},
                     {&source_2, {&pipe_2_1, &pipe_3_1}},
                     {&source_3, {&pipe_1_2, &pipe_3_1}},
                     {&source_4, {&pipe_3_1}}},
                    &sink};
  auto stages = PipelineStagesBuilder(dop, pipeline).Build();

  ASSERT_EQ(stages.size(), 4);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_EQ(stages[1].first.size(), 2);
  ASSERT_EQ(stages[2].first.size(), 1);
  ASSERT_EQ(stages[3].first.size(), 1);

  ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[0].get()), nullptr);
  ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[1].get()), nullptr);
  ASSERT_NE(dynamic_cast<MemorySource*>(stages[2].first[0].get()), nullptr);
  ASSERT_NE(dynamic_cast<DistributedMemorySource*>(stages[3].first[0].get()), nullptr);

  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 4);
  ASSERT_NE(stages[1].second, nullptr);
  ASSERT_EQ(stages[1].second->tasks_.size(), 2);
  ASSERT_NE(stages[2].second, nullptr);
  ASSERT_EQ(stages[2].second->tasks_.size(), 1);
  ASSERT_NE(stages[3].second, nullptr);
  ASSERT_EQ(stages[3].second->tasks_.size(), 1);
}

TEST(ControlFlowTest, OneToOne) {
  size_t dop = 8;
  MemorySource source({{1}});
  IdentityPipe pipe;
  MemorySink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 1);
  ASSERT_EQ(sink.batches_[0], (Batch{1}));
}

TEST(ControlFlowTest, OneToThreeFlat) {
  size_t dop = 8;
  MemorySource source({{1}});
  TimesNFlatPipe pipe(3);
  MemorySink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 1);
  ASSERT_EQ(sink.batches_[0], (Batch{1, 1, 1}));
}

TEST(ControlFlowTest, OneToThreeSliced) {
  size_t dop = 8;
  MemorySource source({{1}});
  TimesNSlicedPipe pipe(dop, 3);
  MemorySink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  ASSERT_EQ(sink.batches_.size(), 3);
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_EQ(sink.batches_[i], (Batch{1}));
  }
}

TEST(ControlFlowTest, AccumulateThree) {
  {
    size_t dop = 1;
    DistributedMemorySource source(dop, {{1}, {1}, {1}});
    AccumulatePipe pipe(dop, 3);
    MemorySink sink;
    Pipeline pipeline{{{&source, {&pipe}}}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
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
    Pipeline pipeline{{{&source, {&pipe}}}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
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

TEST(ControlFlowTest, BasicBackpressure) {
  size_t dop = 8;
  BackpressureContexts ctx(dop);
  InfiniteSource internal_source({});
  BackpressureDelegateSource source(ctx, &internal_source);
  IdentityPipe internal_pipe;
  BackpressureDelegatePipe pipe(ctx, &internal_pipe);
  BlackHoleSink internal_sink;
  BackpressureDelegateSink sink(dop, ctx, 100, 200, 300, &internal_sink);
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
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

TEST(ControlFlowTest, BasicError) {
  {
    size_t dop = 8;
    InfiniteSource source(Batch{});
    IdentityPipe pipe;
    BlackHoleSink sink;
    ErrorGenerator err_gen(42);
    ErrorDelegateSource err_source(&err_gen, &source);
    Pipeline pipeline{{{&err_source, {&pipe}}}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
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
    ErrorDelegatePipe err_pipe(&err_gen, &pipe);
    Pipeline pipeline{{{&source, {&err_pipe}}}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
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
    ErrorDelegateSink err_sink(&err_gen, &sink);
    Pipeline pipeline{{{&source, {&pipe}}}, &err_sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_NOT_OK(result);
    ASSERT_TRUE(result.status().IsInvalid());
    ASSERT_EQ(result.status().message(), "42");
  }
}

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

TEST(ControlFlowTest, BasicYield) {
  {
    size_t dop = 8;
    MemorySource source({{1}, {1}, {1}});
    IdentityPipe internal_pipe;
    SpillDelegatePipe pipe(dop, &internal_pipe);
    MemorySink sink;
    Pipeline pipeline{{{&source, {&pipe}}}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_OK(result);
    ASSERT_EQ(sink.batches_.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1}));
    }
  }

  {
    size_t dop = 2;
    MemorySource source({{1}, {1}, {1}});
    IdentityPipe internal_pipe;
    SpillDelegatePipe pipe(dop, &internal_pipe);
    MemorySink sink;
    Pipeline pipeline{{{&source, {&pipe}}}, &sink};
    FollyFutureScheduler scheduler(4);
    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_OK(result);
    ASSERT_EQ(sink.batches_.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(sink.batches_[i], (Batch{1}));
    }
  }

  {
    size_t dop = 8;
    MemorySource source({{1}, {1}, {1}, {1}});
    IdentityPipe internal_pipe;
    SpillDelegatePipe pipe(dop, &internal_pipe);
    MemorySink sink;
    Pipeline pipeline{{{&source, {&pipe}}}, &sink};

    folly::CPUThreadPoolExecutor cpu_executor(4);
    size_t num_io_threads = 1;
    folly::IOThreadPoolExecutor io_executor(num_io_threads);
    TaskObserver observer(dop);
    FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor, &observer);

    Driver<FollyFutureDoublePoolScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
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

TEST(ControlFlowTest, Drain) {
  size_t dop = 2;
  MemorySource source({{1}, {1}, {1}, {1}});
  DrainOnlyPipe pipe(dop);
  MemorySink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(1);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  Driver<FollyFutureDoublePoolScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_OK(result);
  ASSERT_EQ(sink.batches_.size(), 4);
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(sink.batches_[i], (Batch{1}));
  }
}

TEST(ControlFlowTest, MultiDrain) {
  size_t dop = 2;
  MemorySource source({{1}, {1}, {1}, {1}});
  DrainOnlyPipe pipe_1(dop);
  DrainOnlyPipe pipe_2(dop);
  DrainOnlyPipe pipe_3(dop);
  MemorySink sink;
  Pipeline pipeline{{{&source, {&pipe_1, &pipe_2, &pipe_3}}}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(1);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  Driver<FollyFutureDoublePoolScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_OK(result);
  ASSERT_EQ(sink.batches_.size(), 4);
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_EQ(sink.batches_[i], (Batch{1}));
  }
}

TEST(ControlFlowTest, DrainBackpressure) {
  size_t dop = 1;
  BackpressureContexts ctx(dop);
  DistributedMemorySource source(dop, {{1}, {1}, {1}, {1}});
  DrainOnlyPipe internal_pipe(dop);
  BackpressureDelegatePipe pipe(ctx, &internal_pipe);
  BlackHoleSink internal_sink;
  BackpressureDelegateSink sink(dop, ctx, 2, 42, 1000, &internal_sink);
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  FollyFutureScheduler scheduler(4);
  Driver<FollyFutureScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
  for (const auto& c : ctx) {
    ASSERT_EQ(c.backpressure, false);
    ASSERT_EQ(c.exit, false);
    ASSERT_EQ(c.source_backpressure, 0);
    ASSERT_EQ(c.source_non_backpressure, 0);
    ASSERT_EQ(c.pipe_backpressure, 0);
    ASSERT_EQ(c.pipe_non_backpressure, 8);
  }
}

TEST(ControlFlowTest, DrainError) {
  size_t dop = 2;
  MemorySource source({{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}});
  ErrorGenerator err_gen(7);
  DrainErrorPipe pipe(&err_gen, dop);
  BlackHoleSink sink;
  Pipeline pipeline{{{&source, {static_cast<ErrorDelegatePipe*>(&pipe)}}}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(1);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  Driver<FollyFutureDoublePoolScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
  ASSERT_NOT_OK(result);
  ASSERT_TRUE(result.status().IsInvalid());
  ASSERT_EQ(result.status().message(), "7");
}

TEST(ControlFlowTest, DrainYield) {
  size_t dop = 8;
  MemorySource source({{1}, {1}, {1}, {1}});
  DrainOnlyPipe internal_pipe(dop);
  DrainSpillDelegatePipe pipe(dop, &internal_pipe);
  MemorySink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};

  folly::CPUThreadPoolExecutor cpu_executor(4);
  size_t num_io_threads = 1;
  folly::IOThreadPoolExecutor io_executor(num_io_threads);
  TaskObserver observer(dop);
  FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor, &observer);

  Driver<FollyFutureDoublePoolScheduler> driver(&scheduler);
  auto result = driver.Run(dop, {pipeline});
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

TEST(ControlFlowTest, ErrorAfterBackpressure) {}
TEST(ControlFlowTest, ErrorAfterDrainBackpressure) {}
TEST(ControlFlowTest, ErrorAfterYield) {}
TEST(ControlFlowTest, ErrorAfterDrainYield) {}
TEST(ControlFlowTest, BackpressureAfterYield) {}
TEST(ControlFlowTest, BackpressureAfterDrainYield) {}
TEST(ControlFlowTest, YieldAfterBackpressure) {}
TEST(ControlFlowTest, YieldAfterDrainBackpressure) {}

class SortTest : public testing::TestWithParam<size_t> {
 protected:
  void Sort(const std::vector<PipeOp*>& pipes) {
    size_t dop = GetParam();
    DistributedMemorySource source(dop, {{1, 10, 100}, {2, 20, 200}, {3, 30, 300}});
    SortSink sink(dop);
    Pipeline pipeline{{{&source, pipes}}, &sink};

    // folly::CPUThreadPoolExecutor cpu_executor(8);
    // folly::IOThreadPoolExecutor io_executor(1);
    // FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);
    FollyFutureScheduler scheduler(8);

    Driver<FollyFutureScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(sink.sorted.size(), dop * 9);
    size_t iter = 0;
    for (size_t elem : {1, 2, 3, 10, 20, 30, 100, 200, 300}) {
      for (size_t i = 0; i < dop; ++i) {
        ASSERT_EQ(sink.sorted[iter++], elem);
      }
    }
  }
};

TEST_P(SortTest, PlainSort) {
  auto dop = GetParam();
  Sort({});
}

INSTANTIATE_TEST_SUITE_P(OperatorTest, SortTest, testing::Range(size_t(1), size_t(43)),
                         [](const auto& param_info) {
                           return std::to_string(param_info.param);
                         });

class FibonacciTest : public testing::TestWithParam<size_t> {
 protected:
  void Fibonacci(const std::vector<PipeOp*>& pipes) {
    size_t dop = 8;
    FibonacciSource source_1, source_2;
    MemorySink sink;
    Pipeline pipeline{{{&source_1, pipes}, {&source_2, pipes}}, &sink};

    folly::CPUThreadPoolExecutor cpu_executor(4);
    folly::IOThreadPoolExecutor io_executor(1);
    FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

    Driver<FollyFutureDoublePoolScheduler> driver(&scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());

    for (const auto& batch : sink.batches_) {
      ASSERT_EQ(batch.size(), 1);
    }
    Batch act(sink.batches_.size());
    std::transform(sink.batches_.begin(), sink.batches_.end(), act.begin(),
                   [](const auto& batch) { return batch[0]; });

    size_t n = GetParam();
    Batch exp(n);
    exp[0] = 1;
    exp[1] = 1;
    for (size_t i = 2; i < n; ++i) {
      exp[i] = exp[i - 1] + exp[i - 2];
    }

    ASSERT_EQ(act, exp);
  }
};

TEST_P(FibonacciTest, PlainFibonacci) {
  auto n = GetParam();
  std::vector<FibonacciPipe> pipe_objs(n - 2);
  std::vector<PipeOp*> pipes(n - 2);
  if (n > 2) {
    std::transform(pipe_objs.begin(), pipe_objs.end(), pipes.begin(),
                   [](auto& pipe_obj) { return static_cast<PipeOp*>(&pipe_obj); });
  }
  Fibonacci(pipes);
}

INSTANTIATE_TEST_SUITE_P(ComplexTest, FibonacciTest,
                         testing::Range(size_t(2), size_t(43)),
                         [](const auto& param_info) {
                           return std::to_string(param_info.param);
                         });
