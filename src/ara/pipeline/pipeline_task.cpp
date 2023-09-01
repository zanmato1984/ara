#include "pipeline_task.h"

#include <ara/common/batch.h>
#include <ara/pipeline/physical_pipeline.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>

namespace ara::pipeline {

using task::TaskContext;
using task::TaskResult;
using task::TaskStatus;

PipelineTask::Plex::Plex(const PipelineTask& task, size_t plex_id, size_t dop)
    : task_(task),
      plex_id_(plex_id),
      dop_(dop),
      source_(task_.pipeline_.Plexes()[plex_id].source_op->Source()),
      pipes_(task_.pipeline_.Plexes()[plex_id].pipe_ops.size()),
      sink_(task_.pipeline_.Plexes()[plex_id].sink_op->Sink()),
      thread_locals_(dop),
      cancelled_(false) {
  const auto& pipe_ops = task_.pipeline_.Plexes()[plex_id].pipe_ops;
  std::transform(pipe_ops.begin(), pipe_ops.end(), pipes_.begin(), [&](auto* pipe_op) {
    return std::make_pair(pipe_op->Pipe(), pipe_op->Drain());
  });
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

#define OBSERVE(Method, ...)                                       \
  if (pipeline_context.pipeline_observer != nullptr) {             \
    ARA_RETURN_NOT_OK(pipeline_context.pipeline_observer->Observe( \
        &PipelineObserver::Method, __VA_ARGS__));                  \
  }

OpResult PipelineTask::Plex::operator()(const PipelineContext& pipeline_context,
                                        const TaskContext& task_context,
                                        ThreadId thread_id) {
  OBSERVE(OnPipelineTaskBegin, task_, plex_id_, pipeline_context, task_context,
          thread_id);

  if (cancelled_) {
    OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context, thread_id,
            OpOutput::Cancelled());
    return OpOutput::Cancelled();
  }

  if (!thread_locals_[thread_id].pipe_stack.empty()) {
    auto pipe_id = thread_locals_[thread_id].pipe_stack.top();
    thread_locals_[thread_id].pipe_stack.pop();
    auto result = Pipe(pipeline_context, task_context, thread_id, pipe_id, std::nullopt);
    OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context, thread_id,
            result);
    return result;
  }

  if (!thread_locals_[thread_id].source_done) {
    OBSERVE(OnPipelineSourceBegin, task_, plex_id_, pipeline_context, task_context,
            thread_id);
    auto result = source_(pipeline_context, task_context, thread_id);
    OBSERVE(OnPipelineSourceEnd, task_, plex_id_, pipeline_context, task_context,
            thread_id, result);
    if (!result.ok()) {
      cancelled_ = true;
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, result);
      return result.status();
    }
    if (result->IsSourceNotReady()) {
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, OpOutput::SourceNotReady());
      return OpOutput::SourceNotReady();
    } else if (result->IsFinished()) {
      thread_locals_[thread_id].source_done = true;
      if (result->GetBatch().has_value()) {
        auto new_result = Pipe(pipeline_context, task_context, thread_id, 0,
                               std::move(result->GetBatch()));
        OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
                thread_id, new_result);
        return new_result;
      }
    } else {
      ARA_CHECK(result->IsSourcePipeHasMore());
      ARA_CHECK(result->GetBatch().has_value());
      auto new_result = Pipe(pipeline_context, task_context, thread_id, 0,
                             std::move(result->GetBatch()));
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, new_result);
      return new_result;
    }
  }

  if (thread_locals_[thread_id].draining >= thread_locals_[thread_id].drains.size()) {
    OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context, thread_id,
            OpOutput::Finished());
    return OpOutput::Finished();
  }

  for (; thread_locals_[thread_id].draining < thread_locals_[thread_id].drains.size();
       ++thread_locals_[thread_id].draining) {
    auto drain_id = thread_locals_[thread_id].drains[thread_locals_[thread_id].draining];
    OBSERVE(OnPipelineDrainBegin, task_, plex_id_, drain_id, pipeline_context,
            task_context, thread_id);
    auto result =
        pipes_[drain_id].second.value()(pipeline_context, task_context, thread_id);
    OBSERVE(OnPipelineDrainEnd, task_, plex_id_, drain_id, pipeline_context, task_context,
            thread_id, result);
    if (!result.ok()) {
      cancelled_ = true;
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, result);
      return result.status();
    }
    if (thread_locals_[thread_id].yield) {
      ARA_CHECK(result->IsPipeYieldBack());
      thread_locals_[thread_id].yield = false;
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, OpOutput::PipeYieldBack());
      return OpOutput::PipeYieldBack();
    }
    if (result->IsPipeYield()) {
      ARA_CHECK(!thread_locals_[thread_id].yield);
      thread_locals_[thread_id].yield = true;
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, OpOutput::PipeYield());
      return OpOutput::PipeYield();
    }
    ARA_CHECK(result->IsSourcePipeHasMore() || result->IsFinished());
    if (result->GetBatch().has_value()) {
      if (result->IsFinished()) {
        ++thread_locals_[thread_id].draining;
      }
      auto new_result = Pipe(pipeline_context, task_context, thread_id, drain_id + 1,
                             std::move(result->GetBatch()));
      OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context,
              thread_id, new_result);
      return new_result;
    }
  }

  OBSERVE(OnPipelineTaskEnd, task_, plex_id_, pipeline_context, task_context, thread_id,
          OpOutput::Finished());
  return OpOutput::Finished();
}

OpResult PipelineTask::Plex::Pipe(const PipelineContext& pipeline_context,
                                  const TaskContext& task_context, ThreadId thread_id,
                                  size_t pipe_id, std::optional<Batch> input) {
  for (size_t i = pipe_id; i < pipes_.size(); ++i) {
    OBSERVE(OnPipelinePipeBegin, task_, plex_id_, i, pipeline_context, task_context,
            thread_id, input);
    auto result =
        pipes_[i].first(pipeline_context, task_context, thread_id, std::move(input));
    OBSERVE(OnPipelinePipeEnd, task_, plex_id_, i, pipeline_context, task_context,
            thread_id, result);
    if (!result.ok()) {
      cancelled_ = true;
      return result.status();
    }
    if (thread_locals_[thread_id].yield) {
      ARA_CHECK(result->IsPipeYieldBack());
      thread_locals_[thread_id].pipe_stack.push(i);
      thread_locals_[thread_id].yield = false;
      return OpOutput::PipeYieldBack();
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

  OBSERVE(OnPipelineSinkBegin, task_, plex_id_, pipeline_context, task_context, thread_id,
          input);
  auto result = sink_(pipeline_context, task_context, thread_id, std::move(input));
  OBSERVE(OnPipelineSinkEnd, task_, plex_id_, pipeline_context, task_context, thread_id,
          result);
  if (!result.ok()) {
    cancelled_ = true;
    return result.status();
  }
  ARA_CHECK(result->IsPipeSinkNeedsMore() || result->IsSinkBackpressure());
  return result;
}

PipelineTask::PipelineTask(const PhysicalPipeline& pipeline, size_t dop)
    : Meta("Task of " + pipeline.Name(), pipeline.Desc()), pipeline_(pipeline) {
  for (size_t i = 0; i < pipeline_.Plexes().size(); ++i) {
    plexes_.emplace_back(*this, i, dop);
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
