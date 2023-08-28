#pragma once

#include <ara/common/observer.h>
#include <ara/pipeline/op/op_output.h>

namespace ara {

class QueryContext;

namespace task {
class TaskContext;
}  // namespace task

namespace pipeline {

class PipelineContext;

class PipelineObserver : public Observer {
 public:
  virtual ~PipelineObserver();

  virtual Status OnXXXBegin(const PipelineContext&) { return Status::OK(); }
  virtual Status OnXXXEnd(const PipelineContext&, const OpResult&) {
    return Status::OK();
  }

 public:
  static std::unique_ptr<PipelineObserver> Make(const QueryContext&);
};

}  // namespace pipeline

}  // namespace ara
