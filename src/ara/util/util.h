#pragma once

#include <ara/common/batch.h>
#include <ara/pipeline/defines.h>
#include <ara/task/defines.h>

#include <arrow/acero/hash_join_node.h>

namespace ara::util {

std::string TaskResultToString(const task::TaskResult&);

std::string OpResultToString(const pipeline::OpResult&);

Result<Batch> KeyPayloadFromInput(const arrow::acero::HashJoinProjectionMaps*,
                                  arrow::MemoryPool*, Batch*);

}  // namespace ara::util
