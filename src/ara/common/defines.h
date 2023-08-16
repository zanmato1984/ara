#pragma once

#include <arrow/api.h>
#include <arrow/result.h>

namespace ara {

using Status = arrow::Status;
template <typename T>
using Result = arrow::Result<T>;
using ThreadId = size_t;

};  // namespace ara
