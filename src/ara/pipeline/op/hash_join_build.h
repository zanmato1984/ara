#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/acero/accumulation_queue.h>
#include <arrow/acero/swiss_join_internal.h>

namespace ara::pipeline {

namespace detail {

class BuildProcessor;
class HashJoin;

};  // namespace detail

class HashJoinBuild : public SinkOp {
 public:
  HashJoinBuild(std::string, std::string);
  ~HashJoinBuild();

  Status Init(const PipelineContext&, std::shared_ptr<detail::HashJoin>);

  PipelineSink Sink(const PipelineContext&);

  task::TaskGroups Frontend(const PipelineContext&);

  std::optional<task::TaskGroup> Backend(const PipelineContext&) { return std::nullopt; }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) { return nullptr; }

 private:
  std::shared_ptr<detail::HashJoin> hash_join_;

  size_t dop_;
  arrow::acero::QueryContext* ctx_;
  arrow::acero::SwissTableForJoinBuild* hash_table_build_;

  std::mutex build_side_mutex_;
  arrow::acero::util::AccumulationQueue build_side_batches_;

  std::unique_ptr<detail::BuildProcessor> build_processor_;
};

}  // namespace ara::pipeline
