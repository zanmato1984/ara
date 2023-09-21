#pragma once

#include <arrow/compute/exec.h>

namespace ara {

using Batch = arrow::compute::ExecBatch;
using Batches = std::vector<Batch>;

}  // namespace ara
