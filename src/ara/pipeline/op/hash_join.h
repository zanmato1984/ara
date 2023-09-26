#pragma once

#include <ara/common/defines.h>

#include <arrow/acero/hash_join_node.h>
#include <arrow/acero/swiss_join_internal.h>

namespace ara::pipeline {

class PipelineContext;

namespace detail {

struct HashJoin {
  size_t dop;
  arrow::acero::QueryContext* ctx;
  int64_t hardware_flags;
  arrow::MemoryPool* pool;

  arrow::acero::JoinType join_type;
  std::vector<arrow::acero::JoinKeyCmp> key_cmp;
  arrow::acero::HashJoinSchema schema_mgr;
  arrow::acero::HashJoinProjectionMaps* schema[2];
  std::shared_ptr<arrow::Schema> output_schema;

  std::vector<arrow::acero::JoinResultMaterialize> materialize;

  arrow::acero::SwissTableForJoin hash_table;
  arrow::acero::SwissTableForJoinBuild hash_table_build;

  Status Init(const PipelineContext&, size_t, arrow::acero::QueryContext*,
              const arrow::acero::HashJoinNodeOptions&, const arrow::Schema&,
              const arrow::Schema&);
};

}  // namespace detail

}  // namespace ara::pipeline
