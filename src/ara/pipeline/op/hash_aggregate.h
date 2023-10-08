#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/acero/aggregate_node.h>
#include <arrow/acero/options.h>
#include <arrow/compute/row/grouper.h>

namespace ara::pipeline {

class HashAggregate : public SinkOp {
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
  Status Consume(ThreadId, arrow::compute::ExecSpan);
  Status Merge();
  Status Finalize();

 private:
  size_t dop_;
  arrow::acero::QueryContext* ctx_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<int> key_field_ids_;
  std::vector<std::vector<arrow::TypeHolder>> agg_src_types_;
  std::vector<std::vector<int>> agg_src_fieldsets_;
  std::vector<arrow::compute::Aggregate> aggs_;
  std::vector<const arrow::compute::HashAggregateKernel*> agg_kernels_;

  struct ThreadLocal {
    std::unique_ptr<arrow::compute::Grouper> grouper;
    std::vector<std::unique_ptr<arrow::compute::KernelState>> agg_states;
  };
  std::vector<ThreadLocal> thread_locals_;

  std::shared_ptr<Batch> output_batch_ = nullptr;
};

}  // namespace ara::pipeline
