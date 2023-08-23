#include "op_output.h"

#include <ara/common/foo.h>
#include <ara/task/task.h>
#include <ara/task/task_status.h>

namespace ara::op {

using ara::task::Task;
using ara::task::TaskContext;
using ara::task::TaskId;
using ara::task::TaskResult;
using ara::task::TaskStatus;

Batch foo() {
  Task task("foo", "bar", [](const TaskContext&, TaskId) -> TaskResult {
    return TaskStatus::Finished();
  });

  TaskContext context;
  auto res = task(context, 0);

  return ara::foo();
}

}  // namespace ara::op
