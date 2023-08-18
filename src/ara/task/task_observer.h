#pragma once

#include <ara/task/defines.h>
#include <ara/task/task.h>

namespace ara::task {

class TaskObserver {
 public:
  virtual ~TaskObserver() = default;

  virtual Status OnTaskBegin(const Task&, const TaskContext&, TaskId) = 0;
  virtual Status OnTaskEnd(const Task&, const TaskContext&, TaskId,
                           const TaskResult&) = 0;

  virtual Status OnContBegin(const Cont&, const TaskContext&) = 0;
  virtual Status OnContEnd(const Cont&, const TaskContext&, const TaskResult&) = 0;
};

// TODO: Some kind of context as factory parameter?
using TaskObserverFactory = std::function<TaskObserver()>;

}  // namespace ara::task
