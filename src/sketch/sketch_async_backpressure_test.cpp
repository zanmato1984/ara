#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>
#include <stack>

#define ARA_DCHECK ARROW_DCHECK
#define ARA_DCHECK_OK ARROW_DCHECK_OK
#define ARA_RETURN_NOT_OK ARROW_RETURN_NOT_OK
#define ARA_ASSIGN_OR_RAISE ARROW_ASSIGN_OR_RAISE

namespace ara::sketch {

using Backpressure = std::any;
using BackpressureClear = std::function<arrow::Status()>;
using BackpressurePair = std::pair<Backpressure, BackpressureClear>;
using GenBackpressurePair = std::function<BackpressurePair()>;

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BACKPRESSURE,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  Backpressure backpressure_;

  TaskStatus(Code code) : code_(code) {}
  TaskStatus(Backpressure backpressure)
      : code_(Code::BACKPRESSURE), backpressure_(std::move(backpressure)) {}

 public:
  bool IsContinue() const { return code_ == Code::CONTINUE; }
  bool IsBackpressure() const { return code_ == Code::BACKPRESSURE; }
  bool IsYield() const { return code_ == Code::YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  Backpressure& GetBackpressure() {
    ARA_DCHECK(IsBackpressure());
    return backpressure_;
  }

 public:
  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Backpressure(Backpressure backpressure) {
    return TaskStatus(std::move(backpressure));
  }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};

using TaskId = size_t;
using ThreadId = size_t;

using TaskResult = arrow::Result<TaskStatus>;
using Task = std::function<TaskResult(TaskId, std::optional<GenBackpressurePair>)>;
using TaskCont = std::function<TaskResult()>;
using TaskNotifyFinish = std::function<arrow::Status()>;
using TaskGroup =
    std::tuple<Task, size_t, std::optional<TaskCont>, std::optional<TaskNotifyFinish>>;
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
  Backpressure backpressure_;

  OperatorResult(Code code, std::optional<Batch> output = std::nullopt)
      : code_(code), output_(std::move(output)) {}
  OperatorResult(Backpressure backpressure)
      : code_(Code::SINK_BACKPRESSURE), backpressure_(std::move(backpressure)) {}

 public:
  bool IsSourceNotReady() const { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() const { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsSinkBackpressure() const { return code_ == Code::SINK_BACKPRESSURE; }
  bool IsPipeYield() const { return code_ == Code::PIPE_YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetOutput() {
    ARA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return output_;
  }

  Backpressure& GetBackpressure() {
    ARA_DCHECK(IsSinkBackpressure());
    return backpressure_;
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
  static OperatorResult SinkBackpressure(Backpressure backpressure) {
    return OperatorResult(std::move(backpressure));
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
using PipelineTaskSink = std::function<arrow::Result<OperatorResult>(
    ThreadId, std::optional<Batch>, std::optional<GenBackpressurePair>)>;

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

struct PhysicalPipelinePlex {
  PipelineTaskSource source;
  std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>> pipes;
  PipelineTaskSink sink;
};

struct PhysicalPipeline {
  std::vector<PhysicalPipelinePlex> plexes;
};

struct LogicalPipeline;
class PipelineStageBuilder;

struct LogicalPipelinePlex {
  SourceOp* source;
  std::vector<PipeOp*> pipes;

 private:
  PhysicalPipelinePlex ToPhysical(SinkOp* sink) const {
    std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>> pipes_(
        pipes.size());
    std::transform(pipes.begin(), pipes.end(), pipes_.begin(), [&](auto* pipe) {
      return std::make_pair(pipe->Pipe(), pipe->Drain());
    });
    return {source->Source(), std::move(pipes_), sink->Sink()};
  }

  friend class LogicalPipeline;
};

struct LogicalPipeline {
  std::vector<LogicalPipelinePlex> plexes;
  SinkOp* sink;

 private:
  PhysicalPipeline ToPhysical() const {
    std::vector<PhysicalPipelinePlex> plexes_(plexes.size());
    std::transform(plexes.begin(), plexes.end(), plexes_.begin(),
                   [&](auto& plex_meta) { return plex_meta.ToPhysical(sink); });
    return {std::move(plexes_)};
  }

  friend class PipelineStageBuilder;
};

struct PipelineStage {
  std::vector<std::unique_ptr<SourceOp>> sources;
  PhysicalPipeline pipeline;
};
using PipelineStages = std::vector<PipelineStage>;

class PipelineStageBuilder {
 public:
  PipelineStageBuilder(const LogicalPipeline& pipeline) : pipeline_(pipeline) {}

  PipelineStages Build() && {
    BuildTopology();
    SortTopology();
    return BuildStages();
  }

 private:
  void BuildTopology() {
    std::unordered_map<PipeOp*, SourceOp*> pipe_source_map;
    auto sink = pipeline_.sink;
    for (auto& plex : pipeline_.plexes) {
      size_t stage = 0;
      topology_.emplace(plex.source,
                        std::pair<size_t, LogicalPipelinePlex>{stage++, plex});
      for (size_t i = 0; i < plex.pipes.size(); ++i) {
        auto pipe = plex.pipes[i];
        if (pipe_source_map.count(pipe) == 0) {
          if (auto pipe_source_up = pipe->Source(); pipe_source_up != nullptr) {
            auto pipe_source = pipe_source_up.get();
            pipe_source_map.emplace(pipe, pipe_source);
            LogicalPipelinePlex new_plex{
                pipe_source,
                std::vector<PipeOp*>(plex.pipes.begin() + i + 1, plex.pipes.end())};
            topology_.emplace(pipe_source, std::pair<size_t, LogicalPipelinePlex>{
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
    PipelineStages stages;
    for (auto& [stage, stage_info] : stages_) {
      auto sources_keepalive = std::move(stage_info.first);
      LogicalPipeline pipeline{std::move(stage_info.second), pipeline_.sink};
      stages.push_back({std::move(sources_keepalive), pipeline.ToPhysical()});
    }
    return stages;
  }

  const LogicalPipeline& pipeline_;

  std::unordered_map<SourceOp*, std::pair<size_t, LogicalPipelinePlex>> topology_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> pipe_sources_keepalive_;
  std::map<size_t, std::pair<std::vector<std::unique_ptr<SourceOp>>,
                             std::vector<LogicalPipelinePlex>>>
      stages_;
};

template <typename PipelinePlexTask>
class PipelineTask {
 public:
  static PipelineTask Make(size_t dop, PhysicalPipeline pipeline) {
    return {dop, std::move(pipeline)};
  }

  PipelineTask(size_t dop, PhysicalPipeline pipeline) {
    for (auto& plex : pipeline.plexes) {
      tasks_.push_back(PipelinePlexTask(dop, std::move(plex)));
    }
  }

  arrow::Result<OperatorResult> Run(ThreadId thread_id,
                                    std::optional<GenBackpressurePair> gen) {
    bool all_finished = true;
    for (auto& task : tasks_) {
      ARA_ASSIGN_OR_RAISE(auto result, task.Run(thread_id, gen));
      if (result.IsFinished()) {
        ARA_DCHECK(!result.GetOutput().has_value());
      } else {
        all_finished = false;
      }
      if (!result.IsFinished() && !result.IsSourceNotReady()) {
        return result;
      }
    }
    if (all_finished) {
      return OperatorResult::Finished(std::nullopt);
    } else {
      return OperatorResult::SourceNotReady();
    }
  }

 private:
  std::vector<PipelinePlexTask> tasks_;
};

class SyncPipelinePlexTask {
 public:
  SyncPipelinePlexTask(size_t dop, PhysicalPipelinePlex plex)
      : dop_(dop), plex_(std::move(plex)), local_states_(dop) {
    std::vector<size_t> drains;
    for (size_t i = 0; i < plex_.pipes.size(); ++i) {
      if (plex_.pipes[i].second.has_value()) {
        drains.push_back(i);
      }
    }
    for (size_t i = 0; i < dop; ++i) {
      local_states_[i].drains = drains;
    }
  }

  SyncPipelinePlexTask(const SyncPipelinePlexTask& other)
      : SyncPipelinePlexTask(other.dop_, other.plex_) {}

  SyncPipelinePlexTask(SyncPipelinePlexTask&& other)
      : SyncPipelinePlexTask(other.dop_, std::move(other.plex_)) {}

  arrow::Result<OperatorResult> Run(ThreadId thread_id,
                                    std::optional<GenBackpressurePair> gen) {
    if (cancelled) {
      return OperatorResult::Cancelled();
    }

    if (!local_states_[thread_id].pipe_stack.empty()) {
      auto pipe_id = local_states_[thread_id].pipe_stack.top();
      local_states_[thread_id].pipe_stack.pop();
      return Pipe(thread_id, pipe_id, std::nullopt, std::move(gen));
    }

    if (!local_states_[thread_id].source_done) {
      auto result = plex_.source(thread_id);
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (result->IsSourceNotReady()) {
        return OperatorResult::SourceNotReady();
      } else if (result->IsFinished()) {
        local_states_[thread_id].source_done = true;
        if (result->GetOutput().has_value()) {
          return Pipe(thread_id, 0, std::move(result->GetOutput()), std::move(gen));
        }
      } else {
        ARA_DCHECK(result->IsSourcePipeHasMore());
        ARA_DCHECK(result->GetOutput().has_value());
        return Pipe(thread_id, 0, std::move(result->GetOutput()), std::move(gen));
      }
    }

    if (local_states_[thread_id].draining >= local_states_[thread_id].drains.size()) {
      return OperatorResult::Finished(std::nullopt);
    }

    for (; local_states_[thread_id].draining < local_states_[thread_id].drains.size();
         ++local_states_[thread_id].draining) {
      auto drain_id = local_states_[thread_id].drains[local_states_[thread_id].draining];
      auto result = plex_.pipes[drain_id].second.value()(thread_id);
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (local_states_[thread_id].yield) {
        ARA_DCHECK(result->IsPipeSinkNeedsMore());
        local_states_[thread_id].yield = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (result->IsPipeYield()) {
        ARA_DCHECK(!local_states_[thread_id].yield);
        local_states_[thread_id].yield = true;
        return OperatorResult::PipeYield();
      }
      ARA_DCHECK(result->IsSourcePipeHasMore() || result->IsFinished());
      if (result->GetOutput().has_value()) {
        if (result->IsFinished()) {
          ++local_states_[thread_id].draining;
        }
        return Pipe(thread_id, drain_id + 1, std::move(result->GetOutput()),
                    std::move(gen));
      }
    }

    return OperatorResult::Finished(std::nullopt);
  }

 private:
  arrow::Result<OperatorResult> Pipe(ThreadId thread_id, size_t pipe_id,
                                     std::optional<Batch> input,
                                     std::optional<GenBackpressurePair> gen) {
    for (size_t i = pipe_id; i < plex_.pipes.size(); ++i) {
      auto result = plex_.pipes[i].first(thread_id, std::move(input));
      if (!result.ok()) {
        cancelled = true;
        return result.status();
      }
      if (local_states_[thread_id].yield) {
        ARA_DCHECK(result->IsPipeSinkNeedsMore());
        local_states_[thread_id].pipe_stack.push(i);
        local_states_[thread_id].yield = false;
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (result->IsPipeYield()) {
        ARA_DCHECK(!local_states_[thread_id].yield);
        local_states_[thread_id].pipe_stack.push(i);
        local_states_[thread_id].yield = true;
        return OperatorResult::PipeYield();
      }
      ARA_DCHECK(result->IsPipeSinkNeedsMore() || result->IsPipeEven() ||
                 result->IsSourcePipeHasMore());
      if (result->IsPipeEven() || result->IsSourcePipeHasMore()) {
        if (result->IsSourcePipeHasMore()) {
          local_states_[thread_id].pipe_stack.push(i);
        }
        ARA_DCHECK(result->GetOutput().has_value());
        input = std::move(result->GetOutput());
      } else {
        return OperatorResult::PipeSinkNeedsMore();
      }
    }

    auto result = plex_.sink(thread_id, std::move(input), std::move(gen));
    if (!result.ok()) {
      cancelled = true;
      return result.status();
    }
    ARA_DCHECK(result->IsPipeSinkNeedsMore() || result->IsSinkBackpressure());
    return result;
  }

 private:
  size_t dop_;
  PhysicalPipelinePlex plex_;

  struct ThreadLocalState {
    std::stack<size_t> pipe_stack;
    bool source_done = false;
    std::vector<size_t> drains;
    size_t draining = 0;
    bool yield = false;
  };
  std::vector<ThreadLocalState> local_states_;
  std::atomic_bool cancelled = false;
};

using SyncPipelineTask = PipelineTask<SyncPipelinePlexTask>;

template <typename PipelineTask, typename Scheduler>
class Driver {
 public:
  using PipelineTaskFactory = std::function<PipelineTask(size_t, PhysicalPipeline)>;

  Driver(PipelineTaskFactory factory, Scheduler* scheduler)
      : factory_(std::move(factory)), scheduler_(scheduler) {}

  TaskResult Run(size_t dop, std::vector<LogicalPipeline> pipelines) {
    for (const auto& pipeline : pipelines) {
      ARA_ASSIGN_OR_RAISE(auto result, RunPipeline(dop, pipeline));
      ARA_DCHECK(result.IsFinished());
    }
    return TaskStatus::Finished();
  }

 private:
  TaskResult RunPipeline(size_t dop, const LogicalPipeline& pipeline) {
    auto stages = PipelineStageBuilder(pipeline).Build();
    auto sink = pipeline.sink;

    auto sink_be = sink->Backend();
    auto sink_be_handle = scheduler_->ScheduleTaskGroups(sink_be);

    for (auto& stage : stages) {
      ARA_ASSIGN_OR_RAISE(auto result, RunStage(dop, stage));
      ARA_DCHECK(result.IsFinished());
    }

    auto sink_fe = sink->Frontend();
    auto sink_fe_handle = scheduler_->ScheduleTaskGroups(sink_fe);
    ARA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroup(sink_fe_handle));
    ARA_DCHECK(result.IsFinished());

    auto sink_be_notify_finish = std::get<3>(sink_be.back());
    if (sink_be_notify_finish.has_value()) {
      ARA_DCHECK(sink_be_notify_finish.value()().ok());
    }
    ARA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroup(sink_be_handle));
    ARA_DCHECK(result.IsFinished());

    return TaskStatus::Finished();
  }

  TaskResult RunStage(size_t dop, const PipelineStage& stage) {
    std::vector<typename Scheduler::TaskGroupHandle> source_be_handles;
    for (auto& source : stage.sources) {
      auto source_be = source->Backend();
      source_be_handles.push_back(scheduler_->ScheduleTaskGroups(source_be));
    }

    for (auto& source : stage.sources) {
      auto source_fe = source->Frontend();
      auto source_fe_handle = scheduler_->ScheduleTaskGroups(source_fe);
      ARA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroup(source_fe_handle));
      ARA_DCHECK(result.IsFinished());
    }

    auto pipeline_task = factory_(dop, stage.pipeline);
    TaskGroup pipeline_task_group{
        [&](ThreadId thread_id, std::optional<GenBackpressurePair> gen) -> TaskResult {
          ARA_ASSIGN_OR_RAISE(auto result, pipeline_task.Run(thread_id, std::move(gen)));
          if (result.IsSinkBackpressure()) {
            return TaskStatus::Backpressure(std::move(result.GetBackpressure()));
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
        dop, std::nullopt, std::nullopt};
    auto pipeline_task_group_handle = scheduler_->ScheduleTaskGroup(pipeline_task_group);
    ARA_ASSIGN_OR_RAISE(auto result,
                        scheduler_->WaitTaskGroup(pipeline_task_group_handle));
    ARA_DCHECK(result.IsFinished());

    for (auto& source_be_handle : source_be_handles) {
      ARA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroup(source_be_handle));
      ARA_DCHECK(result.IsFinished());
    }

    return TaskStatus::Finished();
  }

 private:
  PipelineTaskFactory factory_;
  Scheduler* scheduler_;
};

class FollyFutureScheduler {
 private:
  using ConcreteTask = std::pair<folly::Promise<folly::Unit>, folly::Future<TaskResult>>;
  using TaskGroupPayload = std::vector<TaskResult>;

 public:
  using TaskGroupHandle = std::optional<std::pair<ConcreteTask, TaskGroupPayload>>;

  class TaskObserver {
   public:
    virtual ~TaskObserver() = default;

    virtual void BeforeTaskRun(const Task& task, TaskId task_id) = 0;
    virtual void AfterTaskRun(const Task& task, TaskId task_id,
                              const TaskResult& result) = 0;
    virtual void BeforeTaskBackpressure(const Task& task, TaskId task_id) = 0;
    virtual void AfterTaskBackpressure(const Task& task, TaskId task_id) = 0;
  };

  FollyFutureScheduler(folly::Executor* executor, TaskObserver* observer = nullptr)
      : executor_(executor), observer_(observer) {}

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group) {
    auto& task = std::get<0>(group);
    auto num_tasks = std::get<1>(group);
    auto& task_cont = std::get<2>(group);

    auto [p, f] = folly::makePromiseContract<folly::Unit>(executor_);
    std::vector<folly::Promise<folly::Unit>> task_promises;
    std::vector<folly::Future<TaskResult>> tasks;
    TaskGroupPayload payload(num_tasks);
    for (size_t i = 0; i < num_tasks; ++i) {
      auto [tp, tf] = MakeTask(task, i, payload[i]);
      task_promises.push_back(std::move(tp));
      tasks.push_back(std::move(tf));
      payload[i] = TaskStatus::Continue();
    }
    auto task_group_f = std::move(f)
                            .thenValue([task_promises = std::move(task_promises),
                                        tasks = std::move(tasks)](auto&&) mutable {
                              for (size_t i = 0; i < task_promises.size(); ++i) {
                                task_promises[i].setValue();
                              }
                              return folly::collectAll(tasks);
                            })
                            .thenValue([&task_cont](auto&& try_results) -> TaskResult {
                              for (auto&& try_result : try_results) {
                                ARA_DCHECK(try_result.hasValue());
                                auto result = try_result.value();
                                ARA_RETURN_NOT_OK(result);
                              }
                              if (task_cont.has_value()) {
                                return task_cont.value()();
                              }
                              return TaskStatus::Finished();
                            });
    p.setValue();
    return std::make_pair(std::make_pair(std::move(p), std::move(task_group_f)),
                          std::move(payload));
  }

  TaskGroupHandle ScheduleTaskGroups(const TaskGroups& groups) {
    if (groups.empty()) {
      return std::nullopt;
    }
    auto handle = ScheduleTaskGroup(groups[0]);
    for (size_t i = 1; i < groups.size(); ++i) {
      ARA_DCHECK(!std::get<3>(groups[i]).has_value());
      auto result = WaitTaskGroup(handle);
      ARA_DCHECK(result.ok());
      handle = ScheduleTaskGroup(groups[i]);
    }
    return handle;
  }

  TaskResult WaitTaskGroup(TaskGroupHandle& group) {
    if (!group.has_value()) {
      return TaskStatus::Finished();
    }
    return group.value().first.second.wait().value();
  }

 private:
  ConcreteTask MakeTask(const Task& task, TaskId task_id, TaskResult& result) {
    auto [p, f] = folly::makePromiseContract<folly::Unit>(executor_);
    auto pred = [&]() {
      return result.ok() && !result->IsFinished() && !result->IsCancelled();
    };
    auto thunk = [&, task_id]() {
      if (result->IsBackpressure()) {
        if (observer_) {
          observer_->BeforeTaskBackpressure(task, task_id);
        }
        auto backpressure = std::any_cast<std::shared_ptr<folly::Future<folly::Unit>>>(
            std::move(result->GetBackpressure()));
        return std::move(*backpressure).thenValue([&](auto&&) {
          result = TaskStatus::Continue();
        });
      }
      return folly::via(executor_).then([&, task_id](auto&&) {
        if (observer_) {
          observer_->BeforeTaskRun(task, task_id);
        }
        result = task(task_id, [&, task, task_id]() -> BackpressurePair {
          return MakeBackpressurePair(task, task_id);
        });
        if (observer_) {
          observer_->AfterTaskRun(task, task_id, result);
        }
      });
    };
    auto task_f = std::move(f).thenValue(
        [pred, thunk, &result](auto&&) -> folly::Future<TaskResult> {
          return folly::whileDo(pred, thunk).thenValue([&](auto&&) {
            return std::move(result);
          });
        });
    return {std::move(p), std::move(task_f)};
  }

  BackpressurePair MakeBackpressurePair(const Task& task, TaskId task_id) {
    auto [p, f] = folly::makePromiseContract<folly::Unit>(executor_);
    // Workaround that std::function must be copy-constructible.
    auto p_ptr = std::make_shared<folly::Promise<folly::Unit>>(std::move(p));
    auto f_ptr = std::make_shared<folly::Future<folly::Unit>>(std::move(f));
    auto cb = [&, p_ptr = std::move(p_ptr), task, task_id]() mutable {
      p_ptr->setValue();
      if (observer_) {
        observer_->AfterTaskBackpressure(task, task_id);
      }
      return arrow::Status::OK();
    };
    return std::make_pair(std::any{std::move(f_ptr)}, std::move(cb));
  };

 private:
  folly::Executor* executor_;
  TaskObserver* observer_;
};

class MemorySource : public SourceOp {
 public:
  MemorySource(std::list<Batch> batches) : batches_(std::move(batches)) {}

  MemorySource(const MemorySource& other) : batches_(other.batches_) {}

  MemorySource(MemorySource&& other) : batches_(std::move(other.batches_)) {}

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

class MemorySink : public SinkOp {
 public:
  MemorySink(size_t backpressure_threshold, size_t backend_threshold,
             size_t finish_threshold)
      : backpressure_threshold_(backpressure_threshold),
        backend_threshold_(backend_threshold),
        finish_threshold_(finish_threshold),
        finished_(false) {}

  PipelineTaskSink Sink() override {
    return [&](ThreadId, std::optional<Batch> input,
               std::optional<GenBackpressurePair> gen) -> arrow::Result<OperatorResult> {
      ARA_DCHECK(gen.has_value());
      if (input.has_value()) {
        std::lock_guard<std::mutex> lock(mutex_);
        staging_batches_.push_back(std::move(input.value()));
        if (staging_batches_.size() > backpressure_threshold_) {
          auto backpressure_pair = gen.value()();
          backpressures_.push_back(std::move(backpressure_pair.second));
          return OperatorResult::SinkBackpressure(std::move(backpressure_pair.first));
        }
      }
      return OperatorResult::PipeSinkNeedsMore();
    };
  }

  TaskGroups Frontend() override { return {}; }

  TaskGroups Backend() override {
    auto task = [&](TaskId, std::optional<GenBackpressurePair>) -> TaskResult {
      std::lock_guard<std::mutex> lock(mutex_);

      if (finished_) {
        CommitStagingBatches();
        return TaskStatus::Finished();
      }

      if (staging_batches_.size() <= backpressure_threshold_) {
        ClearBackpressures();
        return TaskStatus::Continue();
      }

      if (staging_batches_.size() >= backend_threshold_) {
        CommitStagingBatches();
      }

      if (staging_batches_.size() + total_batches_.size() == finish_threshold_) {
        CommitStagingBatches();
      }

      return TaskStatus::Continue();
    };

    auto task_notify_finish = [&]() -> arrow::Status {
      finished_ = true;
      return arrow::Status::OK();
    };

    return {{std::move(task), 1, std::nullopt, std::move(task_notify_finish)}};
  }

 private:
  void CommitStagingBatches() {
    total_batches_.insert(total_batches_.end(),
                          std::move_iterator(staging_batches_.begin()),
                          std::move_iterator(staging_batches_.end()));
    staging_batches_.clear();
  }

  void ClearBackpressures() {
    for (auto& bp : backpressures_) {
      ARA_DCHECK_OK(bp());
    }
    backpressures_.clear();
  }

 public:
  size_t backpressure_threshold_;
  size_t backend_threshold_;
  size_t finish_threshold_;

  std::mutex mutex_;
  std::atomic<bool> finished_;

  std::vector<Batch> staging_batches_;
  std::vector<Batch> total_batches_;
  std::vector<BackpressureClear> backpressures_;
};

}  // namespace ara::sketch

using namespace ara::sketch;

class AsyncBackpressureTest : public testing::TestWithParam<size_t> {
 protected:
  void Basic() {
    size_t dop = 4;
    size_t backpressure_batches = 8;
    size_t total_batches = GetParam();
    MemorySource source(std::list<Batch>(total_batches, Batch{1}));
    MemorySink sink(backpressure_batches, backpressure_batches + dop, total_batches);
    LogicalPipeline pipeline{{{&source, {}}}, &sink};
    folly::CPUThreadPoolExecutor executor(8);
    FollyFutureScheduler scheduler(&executor);
    Driver<SyncPipelineTask, FollyFutureScheduler> driver(SyncPipelineTask::Make,
                                                          &scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(sink.total_batches_.size(), total_batches);
  }

  void BasicWithObserver() {
    struct TaskObserver : public FollyFutureScheduler::TaskObserver {
      TaskObserver(size_t dop)
          : backpressure_to_handle_task_ids(dop), backpressure_resolved_task_ids(dop) {
        for (size_t i = 0; i < dop; ++i) {
          backpressure_to_handle_task_ids[i] = 0;
          backpressure_resolved_task_ids[i] = 0;
        }
      }

      void BeforeTaskRun(const Task& task, TaskId task_id) override {}

      void AfterTaskRun(const Task& task, TaskId task_id,
                        const TaskResult& result) override {
        ARA_DCHECK(result.ok());
        if (result->IsBackpressure()) {
          task_hit_backpressures++;
        }
      }

      void BeforeTaskBackpressure(const Task& task, TaskId task_id) override {
        backpressures_to_handle++;
        backpressure_to_handle_task_ids[task_id]++;
      }

      void AfterTaskBackpressure(const Task& task, TaskId task_id) override {
        backpressures_resolved++;
        backpressure_resolved_task_ids[task_id]++;
      }

      std::atomic<size_t> task_hit_backpressures = 0, backpressures_to_handle = 0,
                          backpressures_resolved = 0;
      std::vector<std::atomic<size_t>> backpressure_to_handle_task_ids;
      std::vector<std::atomic<size_t>> backpressure_resolved_task_ids;
    };

    size_t dop = 4;
    size_t backpressure_batches = 8;
    size_t total_batches = GetParam();
    MemorySource source(std::list<Batch>(total_batches, Batch{1}));
    MemorySink sink(backpressure_batches, backpressure_batches + dop, total_batches);
    LogicalPipeline pipeline{{{&source, {}}}, &sink};
    folly::CPUThreadPoolExecutor executor(8);
    TaskObserver observer(dop);
    FollyFutureScheduler scheduler(&executor, &observer);
    Driver<SyncPipelineTask, FollyFutureScheduler> driver(SyncPipelineTask::Make,
                                                          &scheduler);
    auto result = driver.Run(dop, {pipeline});
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_EQ(sink.total_batches_.size(), total_batches);

    size_t backpressure_division_per_task = total_batches / (backpressure_batches + dop);
    size_t backpressure_division = backpressure_division_per_task * dop;
    size_t backpressure_remainder =
        total_batches % (backpressure_batches + dop) -
        std::min(total_batches % (backpressure_batches + dop), backpressure_batches);

    size_t backpressures_exp = backpressure_division + backpressure_remainder;

    if (observer.task_hit_backpressures != backpressures_exp) {
      std::cout << sink.staging_batches_.size() << std::endl;
      std::cout << sink.total_batches_.size() << std::endl;
      std::cout << sink.backpressures_.size() << std::endl;
    }

    ASSERT_EQ(observer.task_hit_backpressures, backpressures_exp);
    ASSERT_EQ(observer.backpressures_to_handle, backpressures_exp);
    ASSERT_EQ(observer.backpressures_resolved, backpressures_exp);

    size_t backpressures_to_handle_remainder = 0, backpressures_resolved_remainder = 0;
    for (size_t i = 0; i < dop; ++i) {
      ASSERT_GE(observer.backpressure_to_handle_task_ids[i],
                backpressure_division_per_task);
      ASSERT_GE(observer.backpressure_resolved_task_ids[i],
                backpressure_division_per_task);
      backpressures_to_handle_remainder +=
          observer.backpressure_to_handle_task_ids[i] - backpressure_division_per_task;
      backpressures_resolved_remainder +=
          observer.backpressure_resolved_task_ids[i] - backpressure_division_per_task;
    }
    ASSERT_EQ(backpressures_to_handle_remainder, backpressure_remainder);
    ASSERT_EQ(backpressures_resolved_remainder, backpressure_remainder);
  }
};

TEST_P(AsyncBackpressureTest, Baisc) { Basic(); }

TEST_P(AsyncBackpressureTest, BaiscWithObserver) { BasicWithObserver(); }

INSTANTIATE_TEST_SUITE_P(AsyncBackpressureSuite, AsyncBackpressureTest,
                         testing::Range(size_t(8), size_t(1024)),
                         [](const auto& param_info) {
                           return std::to_string(param_info.index) + "_" +
                                  std::to_string(param_info.param);
                         });
