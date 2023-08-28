#pragma once

#include <ara/pipeline/op/op_output.h>
#include <ara/task/task_group.h>

namespace ara::pipeline {

class PipelineContext;

using PhysicalSource =
    std::function<OpResult(const PipelineContext&, const task::TaskContext&, ThreadId)>;
using PhysicalPipe = std::function<OpResult(
    const PipelineContext&, const task::TaskContext&, ThreadId, std::optional<Batch>)>;
using PhysicalDrain =
    std::function<OpResult(const PipelineContext&, const task::TaskContext&, ThreadId)>;
using PhysicalSink = std::function<OpResult(
    const PipelineContext&, const task::TaskContext&, ThreadId, std::optional<Batch>)>;

class SourceOp {
 public:
  virtual ~SourceOp() = default;
  virtual PhysicalSource Source() = 0;
  virtual task::TaskGroups Frontend() = 0;
  virtual task::TaskGroup Backend() = 0;
};

class PipeOp {
 public:
  virtual ~PipeOp() = default;
  virtual PhysicalPipe Pipe() = 0;
  virtual std::optional<PhysicalDrain> Drain() = 0;
  virtual std::unique_ptr<SourceOp> ImplicitSource() = 0;
};

class SinkOp {
 public:
  virtual ~SinkOp() = default;
  virtual PhysicalSink Sink() = 0;
  virtual task::TaskGroups Frontend() = 0;
  virtual task::TaskGroup Backend() = 0;
  virtual std::unique_ptr<SourceOp> ImplicitSource() = 0;
};

}  // namespace ara::pipeline
