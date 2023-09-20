#pragma once

#include <arrow/acero/exec_plan.h>
#include <arrow/compute/util.h>

namespace ara {

struct Options {
  size_t source_max_batch_length = arrow::acero::ExecPlan::kMaxBatchSize;
  size_t pipe_max_batch_length = arrow::acero::ExecPlan::kMaxBatchSize;
  size_t minibatch_length = arrow::util::MiniBatch::kMiniBatchLength;
};

}  // namespace ara
