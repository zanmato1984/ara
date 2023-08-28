#pragma once

#include <ara/common/observer.h>
#include <ara/pipeline/op/op_output.h>

namespace ara {

class QueryContext;

namespace task {
class TaskContext;
}  // namespace task

namespace pipeline {

class PipelineTask;
class PipelineContext;

class PipelineObserver : public Observer {
 public:
  virtual ~PipelineObserver() = default;

  virtual Status OnPipelineTaskBegin(const PipelineTask&, size_t, const PipelineContext&,
                                     const task::TaskContext&, ThreadId) {
    return Status::OK();
  }
  virtual Status OnPipelineTaskEnd(const PipelineTask&, size_t, const PipelineContext&,
                                   const task::TaskContext&, ThreadId, const OpResult&) {
    return Status::OK();
  }

  virtual Status OnPipelineSourceBegin(const PipelineTask&, size_t,
                                       const PipelineContext&, const task::TaskContext&,
                                       ThreadId) {
    return Status::OK();
  }
  virtual Status OnPipelineSourceEnd(const PipelineTask&, size_t, const PipelineContext&,
                                     const task::TaskContext&, ThreadId,
                                     const OpResult&) {
    return Status::OK();
  }

  virtual Status OnPipelinePipeBegin(const PipelineTask&, size_t, size_t,
                                     const PipelineContext&, const task::TaskContext&,
                                     ThreadId, const std::optional<Batch>&) {
    return Status::OK();
  }
  virtual Status OnPipelinePipeEnd(const PipelineTask&, size_t, size_t,
                                   const PipelineContext&, const task::TaskContext&,
                                   ThreadId, const OpResult&) {
    return Status::OK();
  }

  virtual Status OnPipelineDrainBegin(const PipelineTask&, size_t, size_t,
                                      const PipelineContext&, const task::TaskContext&,
                                      ThreadId) {
    return Status::OK();
  }
  virtual Status OnPipelineDrainEnd(const PipelineTask&, size_t, size_t,
                                    const PipelineContext&, const task::TaskContext&,
                                    ThreadId, const OpResult&) {
    return Status::OK();
  }

  virtual Status OnPipelineSinkBegin(const PipelineTask&, size_t, const PipelineContext&,
                                     const task::TaskContext&, ThreadId,
                                     const std::optional<Batch>&) {
    return Status::OK();
  }
  virtual Status OnPipelineSinkEnd(const PipelineTask&, size_t, const PipelineContext&,
                                   const task::TaskContext&, ThreadId, const OpResult&) {
    return Status::OK();
  }

 public:
  static std::unique_ptr<PipelineObserver> Make(const QueryContext&);
};

}  // namespace pipeline

}  // namespace ara
