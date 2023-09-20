#include <ara/pipeline/op/op_output.h>
#include <ara/task/task_status.h>
#include <ara/util/util.h>

namespace ara::util {

using pipeline::OpResult;
using task::TaskResult;

std::string TaskResultToString(const TaskResult& result) {
  if (!result.ok()) {
    return result.status().ToString();
  }
  return result->ToString();
}

std::string OpResultToString(const OpResult& result) {
  if (!result.ok()) {
    return result.status().ToString();
  }
  return result->ToString();
}

Result<Batch> KeyPayloadFromInput(const arrow::acero::HashJoinProjectionMaps* schema,
                                  arrow::MemoryPool* pool, Batch* input) {
  Batch projected({}, input->length);
  int num_key_cols = schema->num_cols(arrow::acero::HashJoinProjection::KEY);
  int num_payload_cols = schema->num_cols(arrow::acero::HashJoinProjection::PAYLOAD);
  projected.values.resize(num_key_cols + num_payload_cols);

  auto key_to_input = schema->map(arrow::acero::HashJoinProjection::KEY,
                                  arrow::acero::HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_key_cols; ++icol) {
    const arrow::Datum& value_in = input->values[key_to_input.get(icol)];
    if (value_in.is_scalar()) {
      ARA_ASSIGN_OR_RAISE(
          projected.values[icol],
          MakeArrayFromScalar(*value_in.scalar(), projected.length, pool));
    } else {
      projected.values[icol] = value_in;
    }
  }
  auto payload_to_input = schema->map(arrow::acero::HashJoinProjection::PAYLOAD,
                                      arrow::acero::HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_payload_cols; ++icol) {
    const arrow::Datum& value_in = input->values[payload_to_input.get(icol)];
    if (value_in.is_scalar()) {
      ARA_ASSIGN_OR_RAISE(
          projected.values[num_key_cols + icol],
          MakeArrayFromScalar(*value_in.scalar(), projected.length, pool));
    } else {
      projected.values[num_key_cols + icol] = value_in;
    }
  }

  return projected;
}

}  // namespace ara::util
