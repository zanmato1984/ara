#pragma once

#include <ara/common/defines.h>
#include <ara/task/task_status.h>

namespace ara::task {

using TaskId = size_t;
using TaskResult = Result<TaskStatus>;

class TaskContext;

}  // namespace ara::task
