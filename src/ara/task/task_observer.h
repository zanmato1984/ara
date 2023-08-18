#pragma once

#include <ara/task/defines.h>

namespace ara::task {

class Task;
class Continuation;

class TaskObserver {
 public:
  virtual ~TaskObserver() = default;

  virtual Status OnTaskBegin(const Task&, const TaskContext&, TaskId) {
    return Status::OK();
  }
  virtual Status OnTaskEnd(const Task&, const TaskContext&, TaskId, const TaskResult&) {
    return Status::OK();
  }

  virtual Status OnContinuationBegin(const Continuation&, const TaskContext&) {
    return Status::OK();
  }
  virtual Status OnContinuationEnd(const Continuation&, const TaskContext&,
                                   const TaskResult&) {
    return Status::OK();
  }
};

}  // namespace ara::task
