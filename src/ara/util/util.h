#pragma once

#include <ara/common/batch.h>
#include <ara/pipeline/defines.h>
#include <ara/task/defines.h>

#include <arrow/acero/hash_join_node.h>

namespace ara::util {

std::string TaskResultToString(const task::TaskResult&);

std::string OpResultToString(const pipeline::OpResult&);

Result<Batch> KeyPayloadFromInput(const arrow::acero::HashJoinProjectionMaps*,
                                  arrow::MemoryPool*, Batch*,
                                  size_t scratch_limit = 64 * 1024 * 1024);

}  // namespace ara::util
