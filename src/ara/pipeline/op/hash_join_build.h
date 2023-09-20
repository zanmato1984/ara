#pragma once

#include <ara/pipeline/op/op.h>

#include <arrow/acero/accumulation_queue.h>
#include <arrow/acero/hash_join_node.h>
#include <arrow/acero/swiss_join_internal.h>

namespace ara::pipeline {

namespace detail {

class BuildProcessor {
 public:
  Status Init(int64_t, arrow::MemoryPool*, const arrow::acero::HashJoinProjectionMaps*,
              arrow::acero::JoinType, arrow::acero::SwissTableForJoinBuild*,
              arrow::acero::SwissTableForJoin*, arrow::acero::util::AccumulationQueue*,
              const std::vector<arrow::acero::JoinResultMaterialize*>&);

  Status StartBuild();

  task::TaskResult Build(ThreadId, arrow::util::TempVectorStack*);

  task::TaskResult FinishBuild();

  task::TaskResult StartMerge();

  task::TaskResult Merge(ThreadId, arrow::util::TempVectorStack*);

  task::TaskResult FinishMerge(arrow::util::TempVectorStack*);

 private:
  size_t dop_;
  int64_t hardware_flags_;
  arrow::MemoryPool* pool_;

  const arrow::acero::HashJoinProjectionMaps* schema_;
  arrow::acero::JoinType join_type_;
  arrow::acero::SwissTableForJoinBuild* hash_table_build_;
  arrow::acero::SwissTableForJoin* hash_table_;
  arrow::acero::util::AccumulationQueue* batches_;

  struct ThreadLocalState {
    size_t round = 0;
    arrow::acero::JoinResultMaterialize* materialize = nullptr;
  };
  std::vector<ThreadLocalState> thread_locals_;
};

};  // namespace detail

class HashJoinBuild : public SinkOp {
 public:
  Status Init(size_t dop, arrow::acero::QueryContext* ctx, int64_t hardware_flags,
              arrow::MemoryPool* pool, const arrow::acero::HashJoinProjectionMaps* schema,
              arrow::acero::JoinType join_type,
              arrow::acero::SwissTableForJoinBuild* hash_table_build,
              arrow::acero::SwissTableForJoin* hash_table,
              const std::vector<arrow::acero::JoinResultMaterialize*>& materialize);

  PipelineSink Sink();

  task::TaskGroups Frontend(const PipelineContext&);

  std::optional<task::TaskGroup> Backend(const PipelineContext&) { return std::nullopt; }

  std::unique_ptr<SourceOp> ImplicitSource() { return nullptr; }

 private:
  size_t dop_;
  arrow::acero::QueryContext* ctx_;
  arrow::acero::SwissTableForJoinBuild* hash_table_build_;
  std::mutex build_side_mutex_;
  arrow::acero::util::AccumulationQueue build_side_batches_;
  detail::BuildProcessor build_processor_;
};

}  // namespace ara::pipeline
