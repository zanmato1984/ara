#pragma once

#include <ara/common/batch.h>
#include <ara/common/meta.h>
#include <ara/pipeline/defines.h>
#include <ara/task/task_group.h>

namespace ara::pipeline {

class PipelineContext;

// TODO: Differentiate SourceResult, PipeResult, SinkResult?
using PipelineSource =
    std::function<OpResult(const PipelineContext&, const task::TaskContext&, ThreadId)>;
using PipelinePipe = std::function<OpResult(
    const PipelineContext&, const task::TaskContext&, ThreadId, std::optional<Batch>)>;
using PipelineDrain =
    std::function<OpResult(const PipelineContext&, const task::TaskContext&, ThreadId)>;
using PipelineSink = std::function<OpResult(
    const PipelineContext&, const task::TaskContext&, ThreadId, std::optional<Batch>)>;

class SourceOp : public internal::Meta {
 public:
  using Meta::Meta;
  virtual ~SourceOp() = default;
  virtual PipelineSource Source(const PipelineContext&) = 0;
  virtual task::TaskGroups Frontend(const PipelineContext&) = 0;
  virtual std::optional<task::TaskGroup> Backend(const PipelineContext&) = 0;
};

class PipeOp : public internal::Meta {
 public:
  using Meta::Meta;
  virtual ~PipeOp() = default;
  virtual PipelinePipe Pipe(const PipelineContext&) = 0;
  virtual PipelineDrain Drain(const PipelineContext&) = 0;
  virtual std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) = 0;
};

class SinkOp : public internal::Meta {
 public:
  using Meta::Meta;
  virtual ~SinkOp() = default;
  virtual PipelineSink Sink(const PipelineContext&) = 0;
  virtual task::TaskGroups Frontend(const PipelineContext&) = 0;
  virtual std::optional<task::TaskGroup> Backend(const PipelineContext&) = 0;
  virtual std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) = 0;
};

}  // namespace ara::pipeline
