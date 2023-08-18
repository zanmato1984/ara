#pragma once

#include <ara/task/defines.h>

namespace ara::task {

class Task;
class Continuation;
class TaskGroup;

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

  virtual Status OnTaskGroupBegin(const TaskGroup&, const TaskContext&) {
    return Status::OK();
  }
  virtual Status OnTaskGroupEnd(const TaskGroup&, const TaskContext&, const TaskResult&) {
    return Status::OK();
  }

  virtual Status OnNotifyFinishBegin(const TaskGroup&, const TaskContext&) {
    return Status::OK();
  }
  virtual Status OnNotifyFinishEnd(const TaskGroup&, const TaskContext&,
                                   const Status&) {
    return Status::OK();
  }
};

}  // namespace ara::task
