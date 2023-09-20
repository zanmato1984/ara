#include "hash_join.h"

#include <ara/util/defines.h>

#include <arrow/acero/query_context.h>

namespace ara::pipeline::detail {

using namespace arrow;
using namespace arrow::acero;

Status ValidateHashJoinNodeOptions(const HashJoinNodeOptions& join_options) {
  if (join_options.key_cmp.empty() || join_options.left_keys.empty() ||
      join_options.right_keys.empty()) {
    return Status::Invalid("key_cmp and keys cannot be empty");
  }

  if ((join_options.key_cmp.size() != join_options.left_keys.size()) ||
      (join_options.key_cmp.size() != join_options.right_keys.size())) {
    return Status::Invalid("key_cmp and keys must have the same size");
  }

  return Status::OK();
}

Status HashJoin::Init(const PipelineContext& pipeline_context, size_t dop_,
                      arrow::acero::QueryContext* ctx_,
                      const HashJoinNodeOptions& options_, const Schema& left_schema_,
                      const Schema& right_schema_) {
  dop = dop_;
  ctx = ctx_;
  hardware_flags = ctx->cpu_info()->hardware_flags();
  pool = ctx->memory_pool();

  join_type = options_.join_type;
  key_cmp = options_.key_cmp;

  ARA_RETURN_NOT_OK(ValidateHashJoinNodeOptions(options_));

  if (options_.output_all) {
    ARA_RETURN_NOT_OK(schema_mgr.Init(
        options_.join_type, left_schema_, options_.left_keys, right_schema_,
        options_.right_keys, options_.filter, options_.output_suffix_for_left,
        options_.output_suffix_for_right));
  } else {
    ARA_RETURN_NOT_OK(schema_mgr.Init(
        options_.join_type, left_schema_, options_.left_keys, options_.left_output,
        right_schema_, options_.right_keys, options_.right_output, options_.filter,
        options_.output_suffix_for_left, options_.output_suffix_for_right));
  }

  schema[0] = &schema_mgr.proj_maps[0];
  schema[1] = &schema_mgr.proj_maps[1];

  output_schema = schema_mgr.MakeOutputSchema("", "");

  materialize.resize(dop);
  for (int i = 0; i < dop; ++i) {
    materialize[i].Init(pool, schema[0], schema[1]);
  }

  return Status::OK();
}

}  // namespace ara::pipeline::detail
