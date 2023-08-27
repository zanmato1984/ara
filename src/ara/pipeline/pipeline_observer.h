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
class PipelineContinuation;
class PipelineContext;

class PipelineObserver : public Observer {
 public:
  virtual ~PipelineObserver();

  virtual Status OnPipelineTaskBegin(const PipelineTask&, const PipelineContext&,
                                     const task::TaskContext&, ThreadId) {
    return Status::OK();
  }
  virtual Status OnPipelineTaskEnd(const PipelineTask&, const PipelineContext&,
                                   const task::TaskContext&, ThreadId, const OpResult&) {
    return Status::OK();
  }

  virtual Status OnPipelineContinuationBegin(const PipelineContinuation&,
                                             const PipelineContext&,
                                             const task::TaskContext&) {
    return Status::OK();
  }
  virtual Status OnPipelineContinuationEnd(const PipelineContinuation&,
                                           const PipelineContext&,
                                           const task::TaskContext&, const OpResult&) {
    return Status::OK();
  }

 public:
  static std::unique_ptr<PipelineObserver> Make(const QueryContext&);
};

}  // namespace pipeline

}  // namespace ara
