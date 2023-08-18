#include "task.h"

namespace ara::task {

Status Task::ObserverBegin(TaskObserver* observer, const TaskContext& context,
                           TaskId task_id) const {
  return observer->OnTaskBegin(*this, context, task_id);
}

Status Task::ObserverEnd(TaskObserver* observer, const TaskContext& context,
                         TaskId task_id, const TaskResult& result) const {
  return observer->OnTaskEnd(*this, context, task_id, result);
}

Status Continuation::ObserverBegin(TaskObserver* observer,
                                   const TaskContext& context) const {
  return observer->OnContinuationBegin(*this, context);
}

Status Continuation::ObserverEnd(TaskObserver* observer, const TaskContext& context,
                                 const TaskResult& result) const {
  return observer->OnContinuationEnd(*this, context, result);
}

}  // namespace ara::task
