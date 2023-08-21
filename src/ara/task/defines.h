#pragma once

#include <ara/common/defines.h>

namespace ara::task {

using TaskId = size_t;
class TaskStatus;
using TaskResult = Result<TaskStatus>;
class TaskContext;
class TaskObserver;
class Task;

}  // namespace ara::task
