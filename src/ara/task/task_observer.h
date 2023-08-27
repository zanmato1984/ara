#pragma once

#include <ara/common/observer.h>
#include <ara/common/query_context.h>
#include <ara/task/defines.h>

namespace ara::task {

class Task;
class Continuation;
class TaskGroup;

class TaskObserver : public Observer {
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

  virtual Status OnNotifyFinishBegin(const TaskGroup&, const TaskContext&) {
    return Status::OK();
  }
  virtual Status OnNotifyFinishEnd(const TaskGroup&, const TaskContext&, const Status&) {
    return Status::OK();
  }

 public:
  static std::unique_ptr<ChainedObserver<TaskObserver>> Make(const QueryContext&);
};

}  // namespace ara::task
