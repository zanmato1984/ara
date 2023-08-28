#pragma once

#include <ara/common/defines.h>
#include <ara/common/meta.h>
#include <ara/pipeline/op/op_output.h>
#include <ara/pipeline/physical_pipeline.h>
#include <ara/task/defines.h>

#include <stack>

namespace ara {

namespace task {
class TaskContext;
}  // namespace task

namespace pipeline {

class PipelineContext;

class PipelineTask : public internal::Meta {
 public:
  class Plex {
   public:
    Plex(const PhysicalPipeline&, size_t, size_t);

    Plex(Plex&& other) : Plex(other.pipeline_, other.plex_id_, other.dop_) {}

    OpResult operator()(const PipelineContext&, const task::TaskContext&, ThreadId);

   private:
    OpResult Pipe(const PipelineContext&, const task::TaskContext&, ThreadId, size_t,
                  std::optional<Batch>);

   private:
    const PhysicalPipeline& pipeline_;
    const size_t plex_id_;
    const PhysicalPipeline::Plex& plex_;
    const size_t dop_;

    PipelineSource source_;
    std::vector<std::pair<PipelinePipe, std::optional<PipelineDrain>>> pipes_;
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

  const std::vector<Plex>& Plexes() const { return plexes_; }

 private:
  std::vector<Plex> plexes_;
};

}  // namespace pipeline

}  // namespace ara
