#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/acero/hash_join_node.h>
#include <arrow/acero/swiss_join_internal.h>

namespace ara::pipeline {

namespace detail {

class HashJoin;
class ProbeProcessor;

}  // namespace detail

class HashJoinProbe : public PipeOp {
 public:
  HashJoinProbe(std::string, std::string);
  ~HashJoinProbe();

  Status Init(const PipelineContext&, std::shared_ptr<detail::HashJoin>);

  PipelinePipe Pipe(const PipelineContext&) override;

  PipelineDrain Drain(const PipelineContext&) override;

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override;

 private:
  std::shared_ptr<detail::HashJoin> hash_join_;

  arrow::acero::QueryContext* ctx_;
  arrow::acero::JoinType join_type_;

  struct ThreadLocal {
    std::vector<arrow::compute::KeyColumnArray> temp_column_arrays;
  };
  std::vector<ThreadLocal> thread_locals_;

  std::unique_ptr<detail::ProbeProcessor> probe_processor_;
};

}  // namespace ara::pipeline
