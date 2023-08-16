#pragma once

#include <ara/common/defines.h>
#include <ara/task/task_context.h>
#include <ara/task/task_status.h>

namespace ara::task {

using TaskInstanceId = size_t;
using TaskResult = Result<TaskStatus>;

using TaskImpl = std::function<TaskResult(const TaskContext&, TaskInstanceId)>;

class Task {
 public:
  TaskResult operator()(const TaskContext& context, TaskInstanceId id) const {
    return impl_(context, id);
  }

 private:
  TaskImpl impl_;
};

};  // namespace ara::task
