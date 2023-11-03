#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace ara::pipeline {

class Sort : public SinkOp {
 public:
  using SinkOp::SinkOp;

  Status Init(const PipelineContext&, size_t, arrow::acero::QueryContext*,
              arrow::compute::SortOptions, std::shared_ptr<arrow::Schema>);

  PipelineSink Sink(const PipelineContext&) override;

  task::TaskGroups Frontend(const PipelineContext&) override;

  std::optional<task::TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override;

 private:
  Status DoSort();

 private:
  size_t dop_;
  arrow::acero::QueryContext* ctx_;

  arrow::compute::SortOptions options_;
  std::shared_ptr<arrow::Schema> output_schema_;

  std::mutex mutex_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;

  std::shared_ptr<arrow::Table> table_ = nullptr;
};

}  // namespace ara::pipeline
