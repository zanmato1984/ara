#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/table.h>

namespace ara::pipeline::detail {

class TableParallelSource : public SourceOp {
 public:
  using SourceOp::SourceOp;

  Status Init(const PipelineContext&, size_t, std::shared_ptr<arrow::Table>);

  PipelineSource Source(const PipelineContext&) override;

  task::TaskGroups Frontend(const PipelineContext&) override;

  std::optional<task::TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

 private:
  size_t dop_;
  std::shared_ptr<arrow::Table> table_;

  std::vector<Batch> batches_;
  struct ThreadLocal {
    size_t n = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
};

}  // namespace ara::pipeline::detail
