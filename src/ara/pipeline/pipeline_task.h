#pragma once

#include <ara/common/defines.h>
#include <ara/common/meta.h>
#include <ara/pipeline/op/op.h>
#include <ara/pipeline/op/op_output.h>
#include <ara/task/defines.h>

#include <stack>

namespace ara {

namespace task {
class TaskContext;
}  // namespace task

namespace pipeline {

class PipelineContext;
class PhysicalPipeline;

class PipelineTask : public internal::Meta {
 public:
  class Channel {
   public:
    Channel(const PipelineTask&, size_t, size_t);

    Channel(Channel&& other) : Channel(other.task_, other.channel_id_, other.dop_) {}

    OpResult operator()(const PipelineContext&, const task::TaskContext&, ThreadId);

   private:
    OpResult Pipe(const PipelineContext&, const task::TaskContext&, ThreadId, size_t,
                  std::optional<Batch>);

   private:
    const PipelineTask& task_;
    const size_t channel_id_;
    const size_t dop_;

    PipelineSource source_;
    std::vector<std::pair<PipelinePipe, PipelineDrain>> pipes_;
    PipelineSink sink_;

    struct ThreadLocal {
      std::stack<size_t> pipe_stack;
      bool source_done = false;
      std::vector<size_t> drains;
      size_t draining = 0;
      bool yield = false;
    };
    std::vector<ThreadLocal> thread_locals_;
    std::atomic_bool cancelled_;
  };

  PipelineTask(const PhysicalPipeline&, size_t);

  task::TaskResult operator()(const PipelineContext&, const task::TaskContext&, ThreadId);

  const PhysicalPipeline& Pipeline() const { return pipeline_; }

  const std::vector<Channel>& Channels() const { return channels_; }

 private:
  const PhysicalPipeline& pipeline_;
  std::vector<Channel> channels_;

  struct ThreadLocal {
    ThreadLocal(size_t size) : finished(size, false) {}

    std::vector<bool> finished;
  };
  std::vector<ThreadLocal> thread_locals_;
};

}  // namespace pipeline

}  // namespace ara
