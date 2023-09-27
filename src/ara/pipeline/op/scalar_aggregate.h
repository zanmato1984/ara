#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/acero/aggregate_node.h>
#include <arrow/acero/options.h>

namespace ara::pipeline {

class ScalarAggregate : public SinkOp {
 public:
  using SinkOp::SinkOp;

  Status Init(const PipelineContext&, size_t, arrow::acero::QueryContext*,
              const arrow::acero::AggregateNodeOptions&, const arrow::Schema&);

  PipelineSink Sink(const PipelineContext&) override;

  task::TaskGroups Frontend(const PipelineContext&) override;

  std::optional<task::TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override;

 private:
  size_t dop_;
  arrow::acero::QueryContext* ctx_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<arrow::compute::Aggregate> aggregates_;
  std::vector<std::vector<int>> target_fieldsets_;
  std::vector<const arrow::compute::ScalarAggregateKernel*> kernels_;
  std::vector<std::vector<arrow::TypeHolder>> kernel_intypes_;
  std::vector<std::vector<std::unique_ptr<arrow::compute::KernelState>>> states_;

  std::shared_ptr<Batch> output_batch_ = nullptr;
};

}  // namespace ara::pipeline
