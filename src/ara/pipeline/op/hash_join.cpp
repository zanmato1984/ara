#include "hash_join.h"

#include <ara/util/defines.h>

#include <arrow/array/util.h>
#include <arrow/datum.h>
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

Result<arrow::compute::ExecBatch> HashJoin::KeyPayloadFromInput(
    int side, arrow::compute::ExecBatch* input) const {
  arrow::compute::ExecBatch projected({}, input->length);
  int num_key_cols = schema[side]->num_cols(HashJoinProjection::KEY);
  int num_payload_cols = schema[side]->num_cols(HashJoinProjection::PAYLOAD);
  projected.values.resize(num_key_cols + num_payload_cols);

  auto key_to_input = schema[side]->map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_key_cols; ++icol) {
    const Datum& value_in = input->values[key_to_input.get(icol)];
    if (value_in.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(
          projected.values[icol],
          MakeArrayFromScalar(*value_in.scalar(), projected.length, pool));
    } else {
      projected.values[icol] = value_in;
    }
  }

  auto payload_to_input =
      schema[side]->map(HashJoinProjection::PAYLOAD, HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_payload_cols; ++icol) {
    const Datum& value_in = input->values[payload_to_input.get(icol)];
    if (value_in.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(projected.values[num_key_cols + icol],
                            MakeArrayFromScalar(*value_in.scalar(), projected.length, pool));
    } else {
      projected.values[num_key_cols + icol] = value_in;
    }
  }

  return projected;
}

Status HashJoin::Init(const PipelineContext&, size_t dop_,
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

  temp_stacks.resize(dop);
  static constexpr int64_t kTempStackUsage =
      64 * static_cast<int64_t>(arrow::util::MiniBatch::kMiniBatchLength);
  for (int i = 0; i < dop; ++i) {
    ARA_RETURN_NOT_OK(temp_stacks[i].Init(pool, kTempStackUsage));
  }

  return Status::OK();
}

}  // namespace ara::pipeline::detail
