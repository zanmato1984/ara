#pragma once

#include <arrow/acero/exec_plan.h>
#include <arrow/compute/util.h>

namespace ara {

struct Options {
  size_t source_max_batch_length = arrow::acero::ExecPlan::kMaxBatchSize;
  size_t pipe_max_batch_length = arrow::acero::ExecPlan::kMaxBatchSize;
  size_t mini_batch_length = arrow::util::MiniBatch::kMiniBatchLength;
  // Host batch-entry admission for Rust expression/program scratch only.
  // Native Arrow buffers remain accounted by the query's MemoryPool.
  size_t expression_scratch_limit = 64 * 1024 * 1024;
};

}  // namespace ara
