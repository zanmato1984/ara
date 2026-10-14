#include <ara/pipeline/op/op_output.h>
#include <ara/task/task_status.h>
#include <ara/util/util.h>
#include <arrow/array/util.h>
#include <broken_expr.h>
#include <new>

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
                                  arrow::MemoryPool* pool, Batch* input,
                                  size_t scratch_limit) {
  if (!input || input->length < 0) return Status::Invalid("invalid input batch");
  int num_key_cols = schema->num_cols(arrow::acero::HashJoinProjection::KEY);
  int num_payload_cols = schema->num_cols(arrow::acero::HashJoinProjection::PAYLOAD);
  std::vector<uint32_t> columns;
  columns.reserve(num_key_cols + num_payload_cols);
  auto key_to_input = schema->map(arrow::acero::HashJoinProjection::KEY,
                                  arrow::acero::HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_key_cols; ++icol) {
    columns.push_back(key_to_input.get(icol));
  }
  auto payload_to_input = schema->map(arrow::acero::HashJoinProjection::PAYLOAD,
                                      arrow::acero::HashJoinProjection::INPUT);
  for (int icol = 0; icol < num_payload_cols; ++icol) {
    columns.push_back(payload_to_input.get(icol));
  }
  struct Host {
    Batch* input;
    arrow::MemoryPool* pool;
    Batch output;
    Status status = Status::OK();
  } host{input, pool, Batch({}, input->length)};
  host.output.values.reserve(columns.size());
  // Arrow buffers remain allocated/accounted by pool. Rust owns only handles
  // and expression scratch, admitted against this synchronous batch-entry cap.
  auto materialize =
      +[](void* ptr, uint32_t column, size_t length, size_t* output) noexcept -> int32_t {
    auto& host = *static_cast<Host*>(ptr);
    try {
      const auto& value = host.input->values[column];
      if (value.is_scalar()) {
        auto array = arrow::MakeArrayFromScalar(*value.scalar(),
                                                static_cast<int64_t>(length), host.pool);
        if (!array.ok()) {
          host.status = array.status();
          return 1;
        }
        host.output.values.emplace_back(std::move(array).ValueOrDie());
      } else {
        host.output.values.push_back(value);
      }
      *output = host.output.values.size() - 1;
      return 0;
    } catch (const std::bad_alloc&) {
      host.status = Status::OutOfMemory("native projection allocation failed");
      return 1;
    } catch (...) {
      host.status = Status::UnknownError("native projection callback failed");
      return 1;
    }
  };
  int code = broken_expr_project(input->values.size(), static_cast<size_t>(input->length),
                                 columns.data(), columns.size(), scratch_limit, &host,
                                 materialize);
  if (!host.status.ok()) return host.status;
  if (code == 4)
    return Status::OutOfMemory("broken-expr scratch admission/allocation failed");
  if (code != 0)
    return Status::Invalid("broken-expr projection failed, bridge code ", code);
  return std::move(host.output);
}

}  // namespace ara::util
