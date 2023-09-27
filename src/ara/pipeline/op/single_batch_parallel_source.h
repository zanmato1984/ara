#pragma once

#include <ara/pipeline/op/op.h>

namespace ara::pipeline::detail {

class SingleBatchParallelSource : public SourceOp {
 public:
  using SourceOp::SourceOp;

  Status Init(const PipelineContext&, size_t, std::shared_ptr<Batch>);

  PipelineSource Source(const PipelineContext&) override;

  task::TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<task::TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

 private:
  size_t dop_;
  std::shared_ptr<Batch> batch_;

  struct ThreadLocal {
    size_t n = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
};

}  // namespace ara::pipeline::detail
