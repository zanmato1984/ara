#include "pipeline_task.h"

#include <ara/common/batch.h>
#include <ara/pipeline/physical_pipeline.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>

namespace ara::pipeline {

using task::Resumers;
using task::TaskContext;
using task::TaskResult;
using task::TaskStatus;

PipelineTask::Channel::Channel(const PipelineContext& pipeline_ctx,
                               const PipelineTask& task, size_t channel_id, size_t dop)
    : pipeline_ctx_(pipeline_ctx),
      task_(task),
      channel_id_(channel_id),
      dop_(dop),
      source_(task_.pipeline_.Channels()[channel_id].source_op->Source(pipeline_ctx)),
      pipes_(task_.pipeline_.Channels()[channel_id].pipe_ops.size()),
      sink_(task_.pipeline_.Channels()[channel_id].sink_op->Sink(pipeline_ctx)),
      thread_locals_(dop),
      cancelled_(false) {
  const auto& pipe_ops = task_.pipeline_.Channels()[channel_id].pipe_ops;
  std::transform(pipe_ops.begin(), pipe_ops.end(), pipes_.begin(), [&](auto* pipe_op) {
    return std::make_pair(pipe_op->Pipe(pipeline_ctx), pipe_op->Drain(pipeline_ctx));
  });
  std::vector<size_t> drains;
  for (size_t i = 0; i < pipes_.size(); ++i) {
    if (pipes_[i].second != nullptr) {
      drains.push_back(i);
    }
  }
  for (size_t i = 0; i < dop; ++i) {
    thread_locals_[i].drains = drains;
  }
}

#define OBSERVE(Method, ...)                                                             \
  if (pipeline_ctx.pipeline_observer != nullptr) {                                       \
    ARA_RETURN_NOT_OK(pipeline_ctx.pipeline_observer->Observe(&PipelineObserver::Method, \
                                                              __VA_ARGS__));             \
  }

OpResult PipelineTask::Channel::operator()(const PipelineContext& pipeline_ctx,
                                           const TaskContext& task_ctx,
                                           ThreadId thread_id) {
  OBSERVE(OnPipelineTaskBegin, task_, channel_id_, pipeline_ctx, task_ctx, thread_id);

  if (cancelled_) {
    OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
            OpOutput::Cancelled());
    return OpOutput::Cancelled();
  }

  if (thread_locals_[thread_id].sinking) {
    thread_locals_[thread_id].sinking = false;
    auto result = Sink(pipeline_ctx, task_ctx, thread_id, std::nullopt);
    OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
            result);
    return result;
  }

  if (!thread_locals_[thread_id].pipe_stack.empty()) {
    auto pipe_id = thread_locals_[thread_id].pipe_stack.top();
    thread_locals_[thread_id].pipe_stack.pop();
    auto result = Pipe(pipeline_ctx, task_ctx, thread_id, pipe_id, std::nullopt);
    OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
            result);
    return result;
  }

  if (!thread_locals_[thread_id].source_done) {
    OBSERVE(OnPipelineSourceBegin, task_, channel_id_, pipeline_ctx, task_ctx, thread_id);
    auto result = source_(pipeline_ctx, task_ctx, thread_id);
    OBSERVE(OnPipelineSourceEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
            result);
    if (!result.ok()) {
      cancelled_ = true;
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              result);
      return result.status();
    }
    if (result->IsBlocked()) {
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              result);
      return result;
    } else if (result->IsFinished()) {
      thread_locals_[thread_id].source_done = true;
      if (result->GetBatch().has_value()) {
        auto new_result =
            Pipe(pipeline_ctx, task_ctx, thread_id, 0, std::move(result->GetBatch()));
        OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
                new_result);
        return new_result;
      }
    } else {
      ARA_CHECK(result->IsSourcePipeHasMore());
      ARA_CHECK(result->GetBatch().has_value());
      auto new_result =
          Pipe(pipeline_ctx, task_ctx, thread_id, 0, std::move(result->GetBatch()));
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              new_result);
      return new_result;
    }
  }

  if (thread_locals_[thread_id].draining >= thread_locals_[thread_id].drains.size()) {
    OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
            OpOutput::Finished());
    return OpOutput::Finished();
  }

  for (; thread_locals_[thread_id].draining < thread_locals_[thread_id].drains.size();
       ++thread_locals_[thread_id].draining) {
    auto drain_id = thread_locals_[thread_id].drains[thread_locals_[thread_id].draining];
    OBSERVE(OnPipelineDrainBegin, task_, channel_id_, drain_id, pipeline_ctx, task_ctx,
            thread_id);
    auto result = pipes_[drain_id].second(pipeline_ctx, task_ctx, thread_id);
    OBSERVE(OnPipelineDrainEnd, task_, channel_id_, drain_id, pipeline_ctx, task_ctx,
            thread_id, result);
    if (!result.ok()) {
      cancelled_ = true;
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              result);
      return result.status();
    }
    if (thread_locals_[thread_id].yield) {
      ARA_CHECK(result->IsPipeYieldBack());
      thread_locals_[thread_id].yield = false;
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              OpOutput::PipeYieldBack());
      return OpOutput::PipeYieldBack();
    }
    if (result->IsPipeYield()) {
      ARA_CHECK(!thread_locals_[thread_id].yield);
      thread_locals_[thread_id].yield = true;
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              OpOutput::PipeYield());
      return OpOutput::PipeYield();
    }
    if (result->IsBlocked()) {
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              result);
      return result;
    }
    ARA_CHECK(result->IsSourcePipeHasMore() || result->IsFinished());
    if (result->GetBatch().has_value()) {
      if (result->IsFinished()) {
        ++thread_locals_[thread_id].draining;
      }
      auto new_result = Pipe(pipeline_ctx, task_ctx, thread_id, drain_id + 1,
                             std::move(result->GetBatch()));
      OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
              new_result);
      return new_result;
    }
  }

  OBSERVE(OnPipelineTaskEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
          OpOutput::Finished());
  return OpOutput::Finished();
}

OpResult PipelineTask::Channel::Pipe(const PipelineContext& pipeline_ctx,
                                     const TaskContext& task_ctx, ThreadId thread_id,
                                     size_t pipe_id, std::optional<Batch> input) {
  for (size_t i = pipe_id; i < pipes_.size(); ++i) {
    OBSERVE(OnPipelinePipeBegin, task_, channel_id_, i, pipeline_ctx, task_ctx, thread_id,
            input);
    auto result = pipes_[i].first(pipeline_ctx, task_ctx, thread_id, std::move(input));
    OBSERVE(OnPipelinePipeEnd, task_, channel_id_, i, pipeline_ctx, task_ctx, thread_id,
            result);
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
    if (result->IsBlocked()) {
      thread_locals_[thread_id].pipe_stack.push(i);
      return result;
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

  return Sink(pipeline_ctx, task_ctx, thread_id, std::move(input));
}

OpResult PipelineTask::Channel::Sink(const PipelineContext& pipeline_ctx,
                                     const TaskContext& task_ctx, ThreadId thread_id,
                                     std::optional<Batch> input) {
  OBSERVE(OnPipelineSinkBegin, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
          input);
  auto result = sink_(pipeline_ctx, task_ctx, thread_id, std::move(input));
  OBSERVE(OnPipelineSinkEnd, task_, channel_id_, pipeline_ctx, task_ctx, thread_id,
          result);
  if (!result.ok()) {
    cancelled_ = true;
    return result.status();
  }
  ARA_CHECK(result->IsPipeSinkNeedsMore() || result->IsBlocked());
  if (result->IsBlocked()) {
    thread_locals_[thread_id].sinking = true;
  }
  return result;
}

PipelineTask::PipelineTask(const PipelineContext& pipeline_ctx,
                           const PhysicalPipeline& pipeline, size_t dop)
    : Meta("Task of " + pipeline.Name(), pipeline.Desc()), pipeline_(pipeline) {
  for (size_t i = 0; i < pipeline_.Channels().size(); ++i) {
    channels_.emplace_back(pipeline_ctx, *this, i, dop);
  }
  for (size_t i = 0; i < dop; ++i) {
    thread_locals_.emplace_back(channels_.size());
  }
}

// TODO: Consider policy for pipeline task channel multiplexing.
// For example, an eager policy will aggressively multiplex different channels (sources)
// for sake of performance, whereas a conservative policy will run channels one by one for
// reducing resource (memory/IO) consumption.

// TODO: Currently the task multiplexes channels whenever a channel gets blocked.
// This is an aggressive way to make sure that at least some reasonable amount of work is
// done. However this "amount" could be too much, e.g., a batch makes its way to sink and
// only gets blocked there (i.e., sink is busy), but the next channel will be ran anyhow
// only because it sees its predecessor gets blocked. This can be improved with more
// information about how much work has been done in the current task run.
TaskResult PipelineTask::operator()(const PipelineContext& pipeline_ctx,
                                    const TaskContext& task_ctx, ThreadId thread_id) {
  bool all_finished = true;
  bool all_unfinished_blocked = true;
  Resumers resumers;
  OpResult op_result;
  for (size_t i = 0; i < channels_.size(); ++i) {
    if (thread_locals_[thread_id].finished[i]) {
      continue;
    }
    if (auto& resumer = thread_locals_[thread_id].resumers[i]; resumer != nullptr) {
      if (resumer->IsResumed()) {
        resumer = nullptr;
      } else {
        resumers.push_back(resumer);
        all_finished = false;
        continue;
      }
    }
    auto& channel = channels_[i];
    ARA_ASSIGN_OR_RAISE(op_result, channel(pipeline_ctx, task_ctx, thread_id));
    if (op_result->IsFinished()) {
      ARA_CHECK(!op_result->GetBatch().has_value());
      thread_locals_[thread_id].finished[i] = true;
    } else {
      all_finished = false;
    }
    if (op_result->IsBlocked()) {
      thread_locals_[thread_id].resumers[i] = op_result->GetResumer();
      resumers.push_back(std::move(op_result->GetResumer()));
    } else {
      all_unfinished_blocked = false;
    }
    if (!op_result->IsFinished() && !op_result->IsBlocked()) {
      break;
    }
  }
  if (all_finished) {
    return TaskStatus::Finished();
  } else if (all_unfinished_blocked && !resumers.empty()) {
    ARA_ASSIGN_OR_RAISE(auto awaiter, task_ctx.any_awaiter_factory(std::move(resumers)));
    return TaskStatus::Blocked(std::move(awaiter));
  } else if (!op_result.ok()) {
    return op_result.status();
  } else if (op_result->IsPipeYield()) {
    return TaskStatus::Yield();
  } else if (op_result->IsCancelled()) {
    return TaskStatus::Cancelled();
  }
  return TaskStatus::Continue();
}

}  // namespace ara::pipeline
