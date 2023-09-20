#include "pipeline_observer.h"
#include "physical_pipeline.h"
#include "pipeline_task.h"

#include <ara/util/util.h>

namespace ara::pipeline {

using util::OpResultToString;

class PipelineLogger : public PipelineObserver {
 public:
  Status OnPipelineTaskBegin(const PipelineTask& pipeline_task, size_t channel,
                             const PipelineContext&, const task::TaskContext&,
                             ThreadId thread_id) override {
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " begin";
    return Status::OK();
  }
  Status OnPipelineTaskEnd(const PipelineTask& pipeline_task, size_t channel,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " end with " << OpResultToString(result);
    return Status::OK();
  }

  Status OnPipelineSourceBegin(const PipelineTask& pipeline_task, size_t channel,
                               const PipelineContext&, const task::TaskContext&,
                               ThreadId thread_id) override {
    auto source_name = pipeline_task.Pipeline().Channels()[channel].source_op->Name();
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " source " << source_name << " begin";
    return Status::OK();
  }
  Status OnPipelineSourceEnd(const PipelineTask& pipeline_task, size_t channel,
                             const PipelineContext&, const task::TaskContext&,
                             ThreadId thread_id, const OpResult& result) override {
    auto source_name = pipeline_task.Pipeline().Channels()[channel].source_op->Name();
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " source " << source_name << " end with "
                  << OpResultToString(result);
    return Status::OK();
  }

  Status OnPipelinePipeBegin(const PipelineTask& pipeline_task, size_t channel,
                             size_t pipe, const PipelineContext&,
                             const task::TaskContext&, ThreadId thread_id,
                             const std::optional<Batch>& input) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    auto input_str = input.has_value() ? "non-null" : "null";
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " pipe " << pipe_name << " begin with "
                  << input_str << " input";
    return Status::OK();
  }
  Status OnPipelinePipeEnd(const PipelineTask& pipeline_task, size_t channel, size_t pipe,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " pipe " << pipe_name << " end with "
                  << OpResultToString(result);
    return Status::OK();
  }

  Status OnPipelineDrainBegin(const PipelineTask& pipeline_task, size_t channel,
                              size_t pipe, const PipelineContext&,
                              const task::TaskContext&, ThreadId thread_id) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " drain " << pipe_name << " begin";
    return Status::OK();
  }
  Status OnPipelineDrainEnd(const PipelineTask& pipeline_task, size_t channel,
                            size_t pipe, const PipelineContext&, const task::TaskContext&,
                            ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " drain " << pipe_name << " end with "
                  << OpResultToString(result);
    return Status::OK();
  }

  Status OnPipelineSinkBegin(const PipelineTask& pipeline_task, size_t channel,
                             const PipelineContext&, const task::TaskContext&,
                             ThreadId thread_id,
                             const std::optional<Batch>& input) override {
    auto sink_name = pipeline_task.Pipeline().Channels()[channel].sink_op->Name();
    auto input_str = input.has_value() ? "non-null" : "null";
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " sink " << sink_name << " begin with "
                  << input_str << " input";
    return Status::OK();
  }
  Status OnPipelineSinkEnd(const PipelineTask& pipeline_task, size_t channel,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    auto sink_name = pipeline_task.Pipeline().Channels()[channel].sink_op->Name();
    ARA_LOG(INFO) << "Pipeline task " << pipeline_task.Name() << " channel " << channel
                  << " thread " << thread_id << " sink " << sink_name << " end with "
                  << OpResultToString(result);
    return Status::OK();
  }
};

std::unique_ptr<PipelineObserver> PipelineObserver::Make(const QueryContext&) {
  auto logger = std::make_unique<PipelineLogger>();
  std::vector<std::unique_ptr<PipelineObserver>> observers;
  observers.push_back(std::move(logger));
  return std::make_unique<ChainedObserver<PipelineObserver>>(std::move(observers));
}

}  // namespace ara::pipeline
