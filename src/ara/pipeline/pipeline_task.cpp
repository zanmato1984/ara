#include "pipeline_task.h"

#include <ara/task/task_context.h>
#include <ara/task/task_status.h>

namespace ara::pipeline {

using task::TaskContext;
using task::TaskResult;
using task::TaskStatus;

PipelineTask::Plex::Plex(const PhysicalPipeline& pipeline, size_t plex_id, size_t dop)
    : pipeline_(pipeline),
      plex_id_(plex_id),
      plex_(pipeline.Plexes()[plex_id]),
      dop_(dop),
      source_(plex_.source_op->Source()),
      pipes_(plex_.pipe_ops.size()),
      sink_(plex_.sink_op->Sink()),
      thread_locals_(dop),
      cancelled_(false) {
  std::transform(
      plex_.pipe_ops.begin(), plex_.pipe_ops.end(), pipes_.begin(),
      [&](auto* pipe_op) { return std::make_pair(pipe_op->Pipe(), pipe_op->Drain()); });
  std::vector<size_t> drains;
  for (size_t i = 0; i < pipes_.size(); ++i) {
    if (pipes_[i].second.has_value()) {
      drains.push_back(i);
    }
  }
  for (size_t i = 0; i < dop; ++i) {
    thread_locals_[i].drains = drains;
  }
}

OpResult PipelineTask::Plex::operator()(const PipelineContext& pipeline_context,
                                        const TaskContext& task_context,
                                        ThreadId thread_id) {
  if (cancelled_) {
    return OpOutput::Cancelled();
  }

  if (!thread_locals_[thread_id].pipe_stack.empty()) {
    auto pipe_id = thread_locals_[thread_id].pipe_stack.top();
    thread_locals_[thread_id].pipe_stack.pop();
    return Pipe(pipeline_context, task_context, thread_id, pipe_id, std::nullopt);
  }

  if (!thread_locals_[thread_id].source_done) {
    auto result = source_(pipeline_context, task_context, thread_id);
    if (!result.ok()) {
      cancelled_ = true;
      return result.status();
    }
    if (result->IsSourceNotReady()) {
      return OpOutput::SourceNotReady();
    } else if (result->IsFinished()) {
      thread_locals_[thread_id].source_done = true;
      if (result->GetBatch().has_value()) {
        return Pipe(pipeline_context, task_context, thread_id, 0,
                    std::move(result->GetBatch()));
      }
    } else {
      ARA_CHECK(result->IsSourcePipeHasMore());
      ARA_CHECK(result->GetBatch().has_value());
      return Pipe(pipeline_context, task_context, thread_id, 0,
                  std::move(result->GetBatch()));
    }
  }

  if (thread_locals_[thread_id].draining >= thread_locals_[thread_id].drains.size()) {
    return OpOutput::Finished();
  }

  for (; thread_locals_[thread_id].draining < thread_locals_[thread_id].drains.size();
       ++thread_locals_[thread_id].draining) {
    auto drain_id = thread_locals_[thread_id].drains[thread_locals_[thread_id].draining];
    auto result =
        pipes_[drain_id].second.value()(pipeline_context, task_context, thread_id);
    if (!result.ok()) {
      cancelled_ = true;
      return result.status();
    }
    if (thread_locals_[thread_id].yield) {
      ARA_CHECK(result->IsPipeSinkNeedsMore());
      thread_locals_[thread_id].yield = false;
      return OpOutput::PipeSinkNeedsMore();
    }
    if (result->IsPipeYield()) {
      ARA_CHECK(!thread_locals_[thread_id].yield);
      thread_locals_[thread_id].yield = true;
      return OpOutput::PipeYield();
    }
    ARA_CHECK(result->IsSourcePipeHasMore() || result->IsFinished());
    if (result->GetBatch().has_value()) {
      if (result->IsFinished()) {
        ++thread_locals_[thread_id].draining;
      }
      return Pipe(pipeline_context, task_context, thread_id, drain_id + 1,
                  std::move(result->GetBatch()));
    }
  }

  return OpOutput::Finished();
}

OpResult PipelineTask::Plex::Pipe(const PipelineContext& pipeline_context,
                                  const TaskContext& task_context, ThreadId thread_id,
                                  size_t pipe_id, std::optional<Batch> input) {
  for (size_t i = pipe_id; i < pipes_.size(); ++i) {
    auto result =
        pipes_[i].first(pipeline_context, task_context, thread_id, std::move(input));
    if (!result.ok()) {
      cancelled_ = true;
      return result.status();
    }
    if (thread_locals_[thread_id].yield) {
      ARA_CHECK(result->IsPipeSinkNeedsMore());
      thread_locals_[thread_id].pipe_stack.push(i);
      thread_locals_[thread_id].yield = false;
      return OpOutput::PipeSinkNeedsMore();
    }
    if (result->IsPipeYield()) {
      ARA_CHECK(!thread_locals_[thread_id].yield);
      thread_locals_[thread_id].pipe_stack.push(i);
      thread_locals_[thread_id].yield = true;
      return OpOutput::PipeYield();
    }
    ARA_CHECK(result->IsPipeSinkNeedsMore() || result->IsPipeEven() ||
              result->IsSourcePipeHasMore());
    if (result->IsPipeEven() || result->IsSourcePipeHasMore()) {
      if (result->IsSourcePipeHasMore()) {
        thread_locals_[thread_id].pipe_stack.push(i);
      }
      ARA_CHECK(result->GetBatch().has_value());
      input = std::move(result->GetBatch());
    } else {
      return OpOutput::PipeSinkNeedsMore();
    }
  }

  auto result = sink_(pipeline_context, task_context, thread_id, std::move(input));
  if (!result.ok()) {
    cancelled_ = true;
    return result.status();
  }
  ARA_CHECK(result->IsPipeSinkNeedsMore() || result->IsSinkBackpressure());
  return result;
}

PipelineTask::PipelineTask(const PhysicalPipeline& pipeline, size_t dop)
    : Meta(pipeline.Name(), pipeline.Desc()) {
  for (size_t i = 0; i < pipeline.Plexes().size(); ++i) {
    plexes_.emplace_back(pipeline, i, dop);
  }
}

namespace {

TaskResult OpResultToTaskResult(OpResult op_result) {
  if (!op_result.ok()) {
    return op_result.status();
  }
  if (op_result->IsSinkBackpressure()) {
    return TaskStatus::Backpressure(std::move(op_result->GetBackpressure()));
  }
  if (op_result->IsPipeYield()) {
    return TaskStatus::Yield();
  }
  if (op_result->IsFinished()) {
    return TaskStatus::Finished();
  }
  if (op_result->IsCancelled()) {
    return TaskStatus::Cancelled();
  }
  return TaskStatus::Continue();
}

}  // namespace

TaskResult PipelineTask::operator()(const PipelineContext& pipeline_context,
                                    const TaskContext& task_context, ThreadId thread_id) {
  bool all_finished = true;
  for (auto& plex : plexes_) {
    ARA_ASSIGN_OR_RAISE(auto op_result, plex(pipeline_context, task_context, thread_id));
    if (op_result.IsFinished()) {
      ARA_CHECK(!op_result.GetBatch().has_value());
    } else {
      all_finished = false;
    }
    if (!op_result.IsFinished() && !op_result.IsSourceNotReady()) {
      return OpResultToTaskResult(std::move(op_result));
    }
  }
  if (all_finished) {
    return TaskStatus::Finished();
  } else {
    return TaskStatus::Continue();
  }
}

}  // namespace ara::pipeline
