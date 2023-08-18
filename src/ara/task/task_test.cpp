#include "task.h"
#include "task_context.h"

#include <gtest/gtest.h>

using namespace ara::task;

TEST(TaskTest, Basic) {
  TaskContext context;
  auto task_impl = [](const TaskContext& context, TaskId task_id) -> TaskResult {
    return TaskStatus::Finished();
  };

  Task task(task_impl, "BasicTask", "Do nothing but finish directly");
}
