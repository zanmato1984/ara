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
      auto result = task->Run(thread_id);
      if (!result.ok()) {
        return result.status();
      }
      if (result->IsFinished()) {
        ARRA_DCHECK(!result->GetOutput().has_value());
      }
      if (!result->IsFinished() && !result->IsSourceNotReady()) {
        return result;
      }
    }
    return OperatorResult::Finished(std::nullopt);
  }

  std::vector<std::unique_ptr<PipelinePlexTask>> tasks_;
};

using PipelineTaskStage = std::pair<std::vector<std::unique_ptr<SourceOp>>,
                                    std::unique_ptr<PipelineMultiplexTask>>;
using PipelineTaskStages = std::vector<PipelineTaskStage>;

class PipelineTaskBuilder {
 public:
  PipelineTaskBuilder(const Pipeline& pipeline, size_t dop)
      : pipeline_(pipeline), dop_(dop) {}

  PipelineTaskStages Build() && {
    // auto op_tree = RecoverOpTree();
    // return {VisitOpTree(op_tree.get())->Build(dop_),
    // std::move(pipe_sources_keepalive_)};
    BuildTopology();
    SortTopology();
    return BuildTaskStages();
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

  PipelineTaskStages BuildTaskStages() {
    PipelineTaskStages task_stages;
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

  // struct OpTree {
  //     std::variant<SourceOp*, PipeOp*, SinkOp*> op;
  //     std::optional<SourceOp*> pipe_source;
  //     std::vector<std::unique_ptr<OpTree>> children;
  // };

  // std::unique_ptr<OpTree> RecoverOpTree() {
  //   std::unordered_map<PipeOp*, OpTree*> pipe_nodes;
  //   auto root = std::make_unique<OpTree>(OpTree{pipeline_.second, std::nullopt, {}});
  //   for (auto& sub_pipeline : pipeline_.first) {
  //     sub_pipelines_.emplace(sub_pipeline.source, sub_pipeline);
  //     auto sub_root =
  //         std::make_unique<OpTree>(OpTree{sub_pipeline.source, std::nullopt, {}});
  //     for (size_t i = 0; i < sub_pipeline.pipes.size(); ++i) {
  //       auto pipe = sub_pipeline.pipes[i];
  //       if (pipe_nodes.count(pipe) == 0) {
  //         std::optional<SourceOp*> pipe_source_opt = std::nullopt;
  //         if (auto pipe_source = pipe->Source(); pipe_source != nullptr) {
  //           pipe_source_opt.emplace(pipe_source.get());
  //           sub_pipelines_.emplace(
  //               pipe_source.get(),
  //               SubPipeline{pipe_source.get(),
  //                           std::vector<PipeOp*>(sub_pipeline.pipes.begin() + i + 1,
  //                                                sub_pipeline.pipes.end())});
  //           pipe_sources_keepalive_.push_back(std::move(pipe_source));
  //         }
  //         auto pipe_node =
  //             std::make_unique<OpTree>(OpTree{pipe, std::move(pipe_source_opt), {}});
  //         pipe_nodes[pipe] = pipe_node.get();
  //         pipe_node->children.push_back(std::move(sub_root));
  //         std::swap(sub_root, pipe_node);
  //       } else {
  //         pipe_nodes[pipe]->children.push_back(std::move(sub_root));
  //         break;
  //       }
  //     }
  //     if (sub_root != nullptr) {
  //       root->children.push_back(std::move(sub_root));
  //     }
  //   }
  //   return root;
  // }

  // template <typename>
  // inline static constexpr bool always_false_v = false;

  // struct Builder {
  //   virtual ~Builder() = default;
  //   virtual std::unique_ptr<PipelineExec> Build(size_t dop) = 0;
  // };

  // struct ConcreteBuilder : public Builder {
  //   SubPipeline pipeline;
  //   SinkOp* sink;

  //   ConcreteBuilder(SubPipeline pipeline, SinkOp* sink)
  //       : pipeline(std::move(pipeline)), sink(sink) {}

  //   std::unique_ptr<PipelineExec> Build(size_t dop) override {
  //     std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>> pipes(
  //         pipeline.pipes.size());
  //     std::transform(
  //         pipeline.pipes.begin(), pipeline.pipes.end(), pipes.begin(),
  //         [&](auto* pipe) { return std::make_pair(pipe->Pipe(), pipe->Drain()); });
  //     return std::make_unique<ConcretePipelineExec>(dop, pipeline.source->Source(),
  //                                                   std::move(pipes), sink->Sink());
  //   }
  // };

  // struct SerialBuilder : public Builder {
  //   std::vector<std::unique_ptr<Builder>> children;

  //   SerialBuilder(std::vector<std::unique_ptr<Builder>> children)
  //       : children(std::move(children)) {}

  //   std::unique_ptr<PipelineExec> Build(size_t dop) override {
  //     std::vector<std::unique_ptr<PipelineExec>> pipelines(children.size());
  //     std::transform(children.begin(), children.end(), pipelines.begin(),
  //                    [&](auto& child) { return child->Build(dop); });
  //     return std::make_unique<SerialPipelineExec>(std::move(pipelines));
  //   }
  // };

  // struct HyperBuilder : public Builder {
  //   std::vector<std::unique_ptr<Builder>> children;

  //   HyperBuilder(std::vector<std::unique_ptr<Builder>> children)
  //       : children(std::move(children)) {}

  //   std::unique_ptr<PipelineExec> Build(size_t dop) override {
  //     std::vector<std::unique_ptr<PipelineExec>> pipelines(children.size());
  //     std::transform(children.begin(), children.end(), pipelines.begin(),
  //                    [&](auto& child) { return child->Build(dop); });
  //     return std::make_unique<HyperPipelineExec>(std::move(pipelines));
  //   }
  // };

  // std::unique_ptr<Builder> VisitOpTree(OpTree* op_tree) {
  //   std::vector<std::unique_ptr<Builder>> children(op_tree->children.size());
  //   std::transform(op_tree->children.begin(), op_tree->children.end(),
  //   children.begin(),
  //                  [&](auto& child) { return VisitOpTree(child.get()); });
  //   return std::visit(
  //       [&](auto&& op) -> std::unique_ptr<Builder> {
  //         using T = std::decay_t<decltype(op)>;
  //         if constexpr (std::is_same_v<T, SourceOp*>) {
  //           ARRA_DCHECK(children.empty());
  //           auto pipeline = sub_pipelines_[op];
  //           return std::make_unique<ConcreteBuilder>(std::move(pipeline),
  //                                                    pipeline_.second);
  //         } else if constexpr (std::is_same_v<T, PipeOp*>) {
  //           ARRA_DCHECK(!children.empty());
  //           std::unique_ptr<Builder> child;
  //           if (children.size() == 1) {
  //             child = std::move(children[0]);
  //           } else {
  //             child = std::make_unique<HyperBuilder>(std::move(children));
  //           }
  //           if (!op_tree->pipe_source.has_value()) {
  //             return child;
  //           } else {
  //             if (auto serial_builder = dynamic_cast<SerialBuilder*>(child.get());
  //                 serial_builder != nullptr) {
  //               serial_builder->children.push_back(std::make_unique<ConcreteBuilder>(
  //                   sub_pipelines_[op_tree->pipe_source.value()], pipeline_.second));
  //               return child;
  //             } else {
  //               std::vector<std::unique_ptr<Builder>> children;
  //               children.push_back(std::move(child));
  //               children.push_back(std::make_unique<ConcreteBuilder>(
  //                   sub_pipelines_[op_tree->pipe_source.value()], pipeline_.second));
  //               return std::make_unique<SerialBuilder>(std::move(children));
  //             }
  //           }
  //         } else if constexpr (std::is_same_v<T, SinkOp*>) {
  //           if (children.size() == 1) {
  //             return std::move(children[0]);
  //           } else {
  //             return std::make_unique<HyperBuilder>(std::move(children));
  //           }
  //         } else {
  //           static_assert(always_false_v<T>, "Unknown type in OpTree");
  //         }
  //       },
  //       op_tree->op);
  // }

  const Pipeline& pipeline_;
  size_t dop_;

  std::unordered_map<SourceOp*, std::pair<size_t, PipelinePlex>> topology_;
  std::unordered_map<SourceOp*, std::unique_ptr<SourceOp>> pipe_sources_keepalive_;
  std::map<size_t,
           std::pair<std::vector<std::unique_ptr<SourceOp>>, std::vector<PipelinePlex>>>
      stages_;
  // std::vector<std::unique_ptr<SourceOp>> pipe_sources_keepalive_;
  // std::map<SourceOp*, SubPipeline> sub_pipelines_;
};

// template <typename Scheduler>
// class Driver {
//  public:
//   Driver(std::vector<Pipeline> pipelines, Scheduler* scheduler)
//       : pipelines_(std::move(pipelines)), scheduler_(scheduler) {}

//   TaskResult Run(size_t dop) {
//     for (const auto& pipeline : pipelines_) {
//       ARRA_ASSIGN_OR_RAISE(
//           auto result, RunPipeline(dop, pipeline.source, pipeline.pipes,
//           pipeline.sink));
//       ARRA_DCHECK(result.IsFinished());
//     }
//     return TaskStatus::Finished();
//   }

//  private:
//   TaskResult RunPipeline(size_t dop, SourceOp* source, const std::vector<PipeOp*>&
//   pipes,
//                          SinkOp* sink) {
//     // TODO: Backend should be waited even error happens.
//     auto sink_be = sink->Backend();
//     auto sink_be_handle = scheduler_->ScheduleTaskGroups(sink_be);

//     std::vector<std::unique_ptr<SourceOp>> sources_keepalive;
//     std::vector<std::pair<SourceOp*, size_t>> sources;
//     sources.emplace_back(source, 0);
//     for (size_t i = 0; i < pipes.size(); i++) {
//       if (auto pipe_source = pipes[i]->Source(); pipe_source != nullptr) {
//         sources.emplace_back(pipe_source.get(), i + 1);
//         sources_keepalive.emplace_back(std::move(pipe_source));
//       }
//     }

//     for (const auto& [source, pipe_start] : sources) {
//       ARRA_ASSIGN_OR_RAISE(auto result,
//                            RunPipeline(dop, source, pipes, pipe_start, sink));
//       ARRA_DCHECK(result.IsFinished());
//     }

//     auto sink_fe = sink->Frontend();
//     auto sink_fe_handle = scheduler_->ScheduleTaskGroups(sink_fe);
//     ARRA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroups(sink_fe_handle));
//     ARRA_DCHECK(result.IsFinished());

//     ARRA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroups(sink_be_handle));
//     ARRA_DCHECK(result.IsFinished());

//     return TaskStatus::Finished();
//   }

//   TaskResult RunPipeline(size_t dop, SourceOp* source, const std::vector<PipeOp*>&
//   pipes,
//                          size_t pipe_start, SinkOp* sink) {
//     // TODO: Backend should be waited even error happens.
//     auto source_be = source->Backend();
//     auto source_be_handle = scheduler_->ScheduleTaskGroups(source_be);

//     auto source_fe = source->Frontend();
//     auto source_fe_handle = scheduler_->ScheduleTaskGroups(source_fe);
//     ARRA_ASSIGN_OR_RAISE(auto result, scheduler_->WaitTaskGroups(source_fe_handle));
//     ARRA_DCHECK(result.IsFinished());

//     auto source_source = source->Source();
//     std::vector<std::pair<PipelineTaskPipe, std::optional<PipelineTaskDrain>>>
//         pipe_and_drains;
//     for (size_t i = pipe_start; i < pipes.size(); ++i) {
//       auto pipe_pipe = pipes[i]->Pipe();
//       auto pipe_drain = pipes[i]->Drain();
//       pipe_and_drains.emplace_back(std::move(pipe_pipe), std::move(pipe_drain));
//     }
//     auto sink_sink = sink->Sink();
//     PipelineTask pipeline_task(dop, source_source, pipe_and_drains, sink_sink);
//     TaskGroup pipeline{[&](ThreadId thread_id) { return pipeline_task.Run(thread_id);
//     },
//                        dop, std::nullopt};
//     auto pipeline_handle = scheduler_->ScheduleTaskGroup(pipeline);
//     ARRA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroup(pipeline_handle));
//     ARRA_DCHECK(result.IsFinished());

//     ARRA_ASSIGN_OR_RAISE(result, scheduler_->WaitTaskGroups(source_be_handle));
//     ARRA_DCHECK(result.IsFinished());

//     return TaskStatus::Finished();
//   }

//  private:
//   std::vector<Pipeline> pipelines_;
//   Scheduler* scheduler_;
// };

// class FollyFutureScheduler {
//  private:
//   using ConcreteTask = folly::SemiFuture<TaskResult>;
//   using TaskGroupPayload = std::vector<TaskResult>;

//  public:
//   using TaskGroupHandle = std::pair<folly::Future<TaskResult>, TaskGroupPayload>;
//   using TaskGroupsHandle = std::vector<TaskGroupHandle>;

//   FollyFutureScheduler(size_t num_threads) : executor_(num_threads) {}

//   TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group) {
//     auto& task = std::get<0>(group);
//     auto num_tasks = std::get<1>(group);
//     auto& task_cont = std::get<2>(group);

//     TaskGroupHandle handle{folly::makeFuture(TaskStatus::Finished()),
//                            TaskGroupPayload(num_tasks)};
//     std::vector<ConcreteTask> tasks;
//     for (size_t i = 0; i < num_tasks; ++i) {
//       handle.second[i] = TaskStatus::Continue();
//       tasks.push_back(MakeTask(task, i, handle.second[i]));
//     }
//     handle.first = folly::via(&executor_)
//                        .thenValue([tasks = std::move(tasks)](auto&&) mutable {
//                          return folly::collectAll(tasks);
//                        })
//                        .thenValue([&task_cont](auto&& try_results) -> TaskResult {
//                          for (auto&& try_result : try_results) {
//                            ARRA_DCHECK(try_result.hasValue());
//                            auto result = try_result.value();
//                            ARRA_RETURN_NOT_OK(result);
//                          }
//                          if (task_cont.has_value()) {
//                            return task_cont.value()();
//                          }
//                          return TaskStatus::Finished();
//                        });
//     return std::move(handle);
//   }

//   TaskGroupsHandle ScheduleTaskGroups(const TaskGroups& groups) {
//     TaskGroupsHandle handles;
//     for (const auto& group : groups) {
//       handles.push_back(ScheduleTaskGroup(group));
//     }
//     return handles;
//   }

//   TaskResult WaitTaskGroup(TaskGroupHandle& group) { return group.first.wait().value();
//   }

//   TaskResult WaitTaskGroups(TaskGroupsHandle& groups) {
//     for (auto& group : groups) {
//       ARRA_RETURN_NOT_OK(WaitTaskGroup(group));
//     }
//     return TaskStatus::Finished();
//   }

//  private:
//   static ConcreteTask MakeTask(const Task& task, TaskId task_id, TaskResult& result) {
//     auto pred = [&]() {
//       return result.ok() &&
//              (result->IsContinue() || result->IsBackpressure() || result->IsYield());
//     };
//     auto thunk = [&, task_id]() {
//       return folly::makeSemiFuture().defer(
//           [&, task_id](auto&&) { result = task(task_id); });
//     };
//     return folly::whileDo(pred, thunk).deferValue([&](auto&&) {
//       return std::move(result);
//     });
//   }

//  private:
//   folly::CPUThreadPoolExecutor executor_;
// };

// class FollyFutureDoublePoolScheduler {
//  private:
//   using ConcreteTask = folly::Future<TaskResult>;
//   using TaskGroupPayload = std::vector<TaskResult>;

//  public:
//   using TaskGroupHandle = std::pair<folly::Future<TaskResult>, TaskGroupPayload>;
//   using TaskGroupsHandle = std::vector<TaskGroupHandle>;

//   class TaskObserver {
//    public:
//     virtual ~TaskObserver() = default;

//     virtual void BeforeTaskRun(const Task& task, TaskId task_id) = 0;
//     virtual void AfterTaskRun(const Task& task, TaskId task_id,
//                               const TaskResult& result) = 0;
//   };

//   FollyFutureDoublePoolScheduler(folly::Executor* cpu_executor,
//                                  folly::Executor* io_executor,
//                                  TaskObserver* observer = nullptr)
//       : cpu_executor_(cpu_executor), io_executor_(io_executor), observer_(observer) {}

//   TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group) {
//     auto& task = std::get<0>(group);
//     auto num_tasks = std::get<1>(group);
//     auto& task_cont = std::get<2>(group);

//     TaskGroupHandle handle{folly::makeFuture(TaskStatus::Finished()),
//                            TaskGroupPayload(num_tasks)};
//     std::vector<ConcreteTask> tasks;
//     for (size_t i = 0; i < num_tasks; ++i) {
//       handle.second[i] = TaskStatus::Continue();
//       tasks.push_back(MakeTask(task, i, handle.second[i]));
//     }
//     handle.first = folly::via(cpu_executor_)
//                        .thenValue([tasks = std::move(tasks)](auto&&) mutable {
//                          return folly::collectAll(tasks);
//                        })
//                        .thenValue([&task_cont](auto&& try_results) -> TaskResult {
//                          for (auto&& try_result : try_results) {
//                            ARRA_DCHECK(try_result.hasValue());
//                            auto result = try_result.value();
//                            ARRA_RETURN_NOT_OK(result);
//                          }
//                          if (task_cont.has_value()) {
//                            return task_cont.value()();
//                          }
//                          return TaskStatus::Finished();
//                        });
//     return std::move(handle);
//   }

//   TaskGroupsHandle ScheduleTaskGroups(const TaskGroups& groups) {
//     TaskGroupsHandle handles;
//     for (const auto& group : groups) {
//       handles.push_back(ScheduleTaskGroup(group));
//     }
//     return handles;
//   }

//   TaskResult WaitTaskGroup(TaskGroupHandle& group) { return group.first.wait().value();
//   }

//   TaskResult WaitTaskGroups(TaskGroupsHandle& groups) {
//     for (auto& group : groups) {
//       ARRA_RETURN_NOT_OK(WaitTaskGroup(group));
//     }
//     return TaskStatus::Finished();
//   }

//  private:
//   ConcreteTask MakeTask(const Task& task, TaskId task_id, TaskResult& result) {
//     auto pred = [&]() {
//       return result.ok() && !result->IsFinished() && !result->IsCancelled();
//     };
//     auto thunk = [&, task_id]() {
//       auto* executor = result->IsYield() ? io_executor_ : cpu_executor_;
//       return folly::via(executor).then([&, task_id](auto&&) {
//         if (observer_) {
//           observer_->BeforeTaskRun(task, task_id);
//         }
//         result = task(task_id);
//         if (observer_) {
//           observer_->AfterTaskRun(task, task_id, result);
//         }
//       });
//     };
//     return folly::whileDo(pred, thunk).thenValue([&](auto&&) {
//       return std::move(result);
//     });
//   }

//  private:
//   folly::Executor* cpu_executor_;
//   folly::Executor* io_executor_;
//   TaskObserver* observer_;
// };

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
          return OperatorResult::SourcePipeHasMore(std::move(output));
        } else {
          return OperatorResult::PipeEven(std::move(output));
        }
      }
      ARRA_DCHECK(input.has_value());
      thread_locals_[thread_id].batch.insert(thread_locals_[thread_id].batch.end(),
                                             input.value().begin(), input.value().end());
      if (thread_locals_[thread_id].batch.size() >= n_) {
        return f(thread_id, std::nullopt);
      } else {
        return OperatorResult::PipeSinkNeedsMore();
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
        return OperatorResult::SourcePipeHasMore({});
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
      return OperatorResult::PipeEven(std::move(input.value()));
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
        return OperatorResult::PipeSinkNeedsMore();
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
        return OperatorResult::SinkBackpressure();
      }
      return OperatorResult::PipeSinkNeedsMore();
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
        return OperatorResult::PipeSinkNeedsMore();
      }
      if (thread_locals_[thread_id].batch.has_value()) {
        ARRA_DCHECK(!input.has_value());
        auto output = std::move(thread_locals_[thread_id].batch.value());
        thread_locals_[thread_id].batch = std::nullopt;
        return OperatorResult::PipeEven(std::move(output));
      }
      ARRA_DCHECK(input.has_value());
      thread_locals_[thread_id].batch = std::move(input.value());
      thread_locals_[thread_id].spilling = true;
      return OperatorResult::PipeYield();
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

class DrainErrorPipe : public ErrorPipe, public DrainOnlyPipe {
 public:
  DrainErrorPipe(ErrorGenerator* err_gen, size_t dop)
      : ErrorPipe(err_gen, static_cast<DrainOnlyPipe*>(this)), DrainOnlyPipe(dop) {}

  PipelineTaskPipe Pipe() override { return DrainOnlyPipe::Pipe(); }

 private:
  std::unique_ptr<ErrorGenerator> err_gen_;
};

// class TaskObserver : public FollyFutureDoublePoolScheduler::TaskObserver {
//  public:
//   TaskObserver(size_t dop)
//       : last_results_(dop, TaskStatus::Continue()), io_thread_infos_(dop) {}

//   void BeforeTaskRun(const Task& task, TaskId task_id) override {}

//   void AfterTaskRun(const Task& task, TaskId task_id, const TaskResult& result)
//   override {
//     if (last_results_[task_id].ok() && last_results_[task_id]->IsYield()) {
//       io_thread_infos_[task_id].insert(std::make_pair(
//           folly::getCurrentThreadID(), folly::getCurrentThreadName().value()));
//     }
//     last_results_[task_id] = result;
//   }

//  public:
//   std::vector<TaskResult> last_results_;
//   std::vector<std::unordered_set<std::pair<ThreadId, std::string>>> io_thread_infos_;
// };

}  // namespace arra::sketch

using namespace arra::sketch;

TEST(PipelineTaskBuildTest, EmptyPipeline) {
  size_t dop = 8;
  BlackHoleSink sink;
  Pipeline pipeline{{}, &sink};
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();
  ASSERT_EQ(stages.size(), 0);
}

TEST(PipelineTaskBuildTest, SinglePlexPipeline) {
  size_t dop = 8;
  InfiniteSource source({});
  IdentityPipe pipe;
  BlackHoleSink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();
  ASSERT_EQ(stages.size(), 1);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 1);
}

TEST(PipelineTaskBuildTest, DoublePlexPipeline) {
  size_t dop = 8;
  InfiniteSource source_1({}), source_2({});
  IdentityPipe pipe;
  BlackHoleSink sink;
  Pipeline pipeline{{{&source_1, {&pipe}}, {&source_2, {&pipe}}}, &sink};
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();
  ASSERT_EQ(stages.size(), 1);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 2);
}

TEST(PipelineTaskBuildTest, DoubleStagePipeline) {
  size_t dop = 8;
  InfiniteSource source({});
  IdentityWithAnotherSourcePipe pipe(std::make_unique<InfiniteSource>(Batch{}));
  BlackHoleSink sink;
  Pipeline pipeline{{{&source, {&pipe}}}, &sink};
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();
  ASSERT_EQ(stages.size(), 2);
  ASSERT_TRUE(stages[0].first.empty());
  ASSERT_EQ(stages[1].first.size(), 1);
  ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].first[0].get()), nullptr);
  ASSERT_NE(stages[0].second, nullptr);
  ASSERT_EQ(stages[0].second->tasks_.size(), 1);
  ASSERT_NE(stages[1].second, nullptr);
  ASSERT_EQ(stages[1].second->tasks_.size(), 1);
}

TEST(PipelineTaskBuildTest, DoubleStageDoublePlexPipeline) {
  size_t dop = 8;
  InfiniteSource source_1({}), source_2({});
  IdentityWithAnotherSourcePipe pipe_1(std::make_unique<InfiniteSource>(Batch{})),
      pipe_2(std::make_unique<MemorySource>(std::list<Batch>()));
  auto pipe_source_1 = pipe_1.source_.get(), pipe_source_2 = pipe_2.source_.get();
  BlackHoleSink sink;
  Pipeline pipeline{{{&source_1, {&pipe_1}}, {&source_2, {&pipe_2}}}, &sink};
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();
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

TEST(PipelineTaskBuildTest, TrippleStagePipeline) {
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
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();
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

TEST(PipelineTaskBuildTest, OddQuadroStagePipeline) {
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
  auto stages = PipelineTaskBuilder(pipeline, dop).Build();

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

// TEST(SketchTest, OneToOne) {
//   size_t dop = 8;
//   MemorySource source({{1}});
//   IdentityPipe pipe;
//   MemorySink sink;
//   Pipeline pipeline{&source, {&pipe}, &sink};
//   FollyFutureScheduler scheduler(4);
//   Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_OK(result);
//   ASSERT_TRUE(result->IsFinished());
//   ASSERT_EQ(sink.batches_.size(), 1);
//   ASSERT_EQ(sink.batches_[0], (Batch{1}));
// }

// TEST(SketchTest, OneToThreeFlat) {
//   size_t dop = 8;
//   MemorySource source({{1}});
//   TimesNFlatPipe pipe(3);
//   MemorySink sink;
//   Pipeline pipeline{&source, {&pipe}, &sink};
//   FollyFutureScheduler scheduler(4);
//   Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_OK(result);
//   ASSERT_TRUE(result->IsFinished());
//   ASSERT_EQ(sink.batches_.size(), 1);
//   ASSERT_EQ(sink.batches_[0], (Batch{1, 1, 1}));
// }

// TEST(SketchTest, OneToThreeSliced) {
//   size_t dop = 8;
//   MemorySource source({{1}});
//   TimesNSlicedPipe pipe(dop, 3);
//   MemorySink sink;
//   Pipeline pipeline{&source, {&pipe}, &sink};
//   FollyFutureScheduler scheduler(4);
//   Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_OK(result);
//   ASSERT_TRUE(result->IsFinished());
//   ASSERT_EQ(sink.batches_.size(), 3);
//   for (size_t i = 0; i < 3; ++i) {
//     ASSERT_EQ(sink.batches_[i], (Batch{1}));
//   }
// }

// TEST(SketchTest, AccumulateThree) {
//   {
//     size_t dop = 8;
//     DistributedMemorySource source(dop, {{1}, {1}, {1}});
//     AccumulatePipe pipe(dop, 3);
//     MemorySink sink;
//     Pipeline pipeline{&source, {&pipe}, &sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_OK(result);
//     ASSERT_TRUE(result->IsFinished());
//     ASSERT_EQ(sink.batches_.size(), dop);
//     for (size_t i = 0; i < dop; ++i) {
//       ASSERT_EQ(sink.batches_[i], (Batch{1, 1, 1}));
//     }
//   }

//   {
//     size_t dop = 8;
//     DistributedMemorySource source(dop, {{1}, {1}, {1}, {1}, {1}});
//     AccumulatePipe pipe(dop, 3);
//     MemorySink sink;
//     Pipeline pipeline{&source, {&pipe}, &sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_OK(result);
//     ASSERT_TRUE(result->IsFinished());
//     ASSERT_EQ(sink.batches_.size(), dop * 2);
//     std::sort(sink.batches_.begin(), sink.batches_.end(),
//               [](const auto& lhs, const auto& rhs) { return lhs.size() < rhs.size();
//               });
//     for (size_t i = 0; i < dop; ++i) {
//       ASSERT_EQ(sink.batches_[i], (Batch{1, 1}));
//     }
//     for (size_t i = dop; i < dop * 2; ++i) {
//       ASSERT_EQ(sink.batches_[i], (Batch{1, 1, 1}));
//     }
//   }
// }

// TEST(SketchTest, BasicBackpressure) {
//   size_t dop = 8;
//   BackpressureContexts ctx(dop);
//   BackpressureSource source(ctx);
//   BackpressurePipe pipe(ctx);
//   BackpressureSink sink(dop, ctx, 100, 200, 300);
//   Pipeline pipeline{&source, {&pipe}, &sink};
//   FollyFutureScheduler scheduler(4);
//   Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_OK(result);
//   ASSERT_TRUE(result->IsFinished());
//   for (const auto& c : ctx) {
//     ASSERT_EQ(c.backpressure, false);
//     ASSERT_EQ(c.exit, true);
//     ASSERT_EQ(c.source_backpressure, 0);
//     ASSERT_EQ(c.source_non_backpressure, 201);
//     ASSERT_EQ(c.pipe_backpressure, 0);
//     ASSERT_EQ(c.pipe_non_backpressure, 200);
//   }
// }

// TEST(SketchTest, BasicError) {
//   {
//     size_t dop = 8;
//     InfiniteSource source(Batch{});
//     IdentityPipe pipe;
//     BlackHoleSink sink;
//     ErrorGenerator err_gen(42);
//     ErrorSource err_source(&err_gen, &source);
//     Pipeline pipeline{&err_source, {&pipe}, &sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_NOT_OK(result);
//     ASSERT_TRUE(result.status().IsInvalid());
//     ASSERT_EQ(result.status().message(), "42");
//   }

//   {
//     size_t dop = 8;
//     InfiniteSource source(Batch{});
//     IdentityPipe pipe;
//     BlackHoleSink sink;
//     ErrorGenerator err_gen(42);
//     ErrorPipe err_pipe(&err_gen, &pipe);
//     Pipeline pipeline{&source, {&err_pipe}, &sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_NOT_OK(result);
//     ASSERT_TRUE(result.status().IsInvalid());
//     ASSERT_EQ(result.status().message(), "42");
//   }

//   {
//     size_t dop = 8;
//     InfiniteSource source(Batch{});
//     IdentityPipe pipe;
//     BlackHoleSink sink;
//     ErrorGenerator err_gen(42);
//     ErrorSink err_sink(&err_gen, &sink);
//     Pipeline pipeline{&source, {&pipe}, &err_sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_NOT_OK(result);
//     ASSERT_TRUE(result.status().IsInvalid());
//     ASSERT_EQ(result.status().message(), "42");
//   }
// }

// TEST(SketchTest, BasicYield) {
//   {
//     size_t dop = 8;
//     MemorySource source({{1}, {1}, {1}});
//     SpillThruPipe pipe(dop);
//     MemorySink sink;
//     Pipeline pipeline{&source, {&pipe}, &sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_OK(result);
//     ASSERT_EQ(sink.batches_.size(), 3);
//     for (size_t i = 0; i < 3; ++i) {
//       ASSERT_EQ(sink.batches_[i], (Batch{1}));
//     }
//   }

//   {
//     size_t dop = 2;
//     MemorySource source({{1}, {1}, {1}});
//     SpillThruPipe pipe(dop);
//     MemorySink sink;
//     Pipeline pipeline{&source, {&pipe}, &sink};
//     FollyFutureScheduler scheduler(4);
//     Driver<FollyFutureScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_OK(result);
//     ASSERT_EQ(sink.batches_.size(), 3);
//     for (size_t i = 0; i < 3; ++i) {
//       ASSERT_EQ(sink.batches_[i], (Batch{1}));
//     }
//   }

//   {
//     size_t dop = 8;
//     MemorySource source({{1}, {1}, {1}, {1}});
//     SpillThruPipe pipe(dop);
//     MemorySink sink;
//     Pipeline pipeline{&source, {&pipe}, &sink};

//     folly::CPUThreadPoolExecutor cpu_executor(4);
//     size_t num_io_threads = 1;
//     folly::IOThreadPoolExecutor io_executor(num_io_threads);
//     TaskObserver observer(dop);
//     FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor, &observer);

//     Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
//     auto result = driver.Run(dop);
//     ASSERT_OK(result);
//     ASSERT_EQ(sink.batches_.size(), 4);
//     for (size_t i = 0; i < 4; ++i) {
//       ASSERT_EQ(sink.batches_[i], (Batch{1}));
//     }

//     std::unordered_set<std::pair<ThreadId, std::string>> io_thread_info;
//     for (size_t i = 0; i < dop; ++i) {
//       std::copy(observer.io_thread_infos_[i].begin(),
//       observer.io_thread_infos_[i].end(),
//                 std::inserter(io_thread_info, io_thread_info.end()));
//     }
//     ASSERT_EQ(io_thread_info.size(), num_io_threads);
//     ASSERT_EQ(io_thread_info.begin()->second.substr(0, 12), "IOThreadPool");
//   }
// }

// TEST(SketchTest, Drain) {
//   size_t dop = 2;
//   MemorySource source({{1}, {1}, {1}, {1}});
//   DrainOnlyPipe pipe(dop);
//   MemorySink sink;
//   Pipeline pipeline{&source, {&pipe}, &sink};

//   folly::CPUThreadPoolExecutor cpu_executor(4);
//   folly::IOThreadPoolExecutor io_executor(1);
//   FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

//   Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_OK(result);
//   ASSERT_EQ(sink.batches_.size(), 4);
//   for (size_t i = 0; i < 4; ++i) {
//     ASSERT_EQ(sink.batches_[i], (Batch{1}));
//   }
// }

// TEST(SketchTest, MultiDrain) {
//   size_t dop = 2;
//   MemorySource source({{1}, {1}, {1}, {1}});
//   DrainOnlyPipe pipe_1(dop);
//   DrainOnlyPipe pipe_2(dop);
//   DrainOnlyPipe pipe_3(dop);
//   MemorySink sink;
//   Pipeline pipeline{&source, {&pipe_1, &pipe_2, &pipe_3}, &sink};

//   folly::CPUThreadPoolExecutor cpu_executor(4);
//   folly::IOThreadPoolExecutor io_executor(1);
//   FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

//   Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_OK(result);
//   ASSERT_EQ(sink.batches_.size(), 4);
//   for (size_t i = 0; i < 4; ++i) {
//     ASSERT_EQ(sink.batches_[i], (Batch{1}));
//   }
// }

// TEST(SketchTest, DrainBackpressure) {}

// TEST(SketchTest, DrainError) {
//   size_t dop = 2;
//   MemorySource source({{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}});
//   ErrorGenerator err_gen(7);
//   DrainErrorPipe pipe(&err_gen, dop);
//   BlackHoleSink sink;
//   Pipeline pipeline{&source, {static_cast<ErrorPipe*>(&pipe)}, &sink};

//   folly::CPUThreadPoolExecutor cpu_executor(4);
//   folly::IOThreadPoolExecutor io_executor(1);
//   FollyFutureDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

//   Driver<FollyFutureDoublePoolScheduler> driver({pipeline}, &scheduler);
//   auto result = driver.Run(dop);
//   ASSERT_NOT_OK(result);
//   ASSERT_TRUE(result.status().IsInvalid());
//   ASSERT_EQ(result.status().message(), "7");
// }

// TEST(SketchTest, ErrorAfterBackpressure) {}

// TEST(SketchTest, ErrorAfterBackpressure) {}
