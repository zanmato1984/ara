#include "task.h"
#include "task_observer.h"

namespace ara::task {

Status Task::ObserverBegin(ChainedObserver<TaskObserver>* observer,
                           const TaskContext& context, TaskId task_id) const {
  return observer->Observe(&TaskObserver::OnTaskBegin, *this, context, task_id);
}

Status Task::ObserverEnd(ChainedObserver<TaskObserver>* observer,
                         const TaskContext& context, TaskId task_id,
                         const TaskResult& result) const {
  return observer->Observe(&TaskObserver::OnTaskEnd, *this, context, task_id, result);
}

Status Continuation::ObserverBegin(ChainedObserver<TaskObserver>* observer,
                                   const TaskContext& context) const {
  return observer->Observe(&TaskObserver::OnContinuationBegin, *this, context);
}

Status Continuation::ObserverEnd(ChainedObserver<TaskObserver>* observer,
                                 const TaskContext& context,
                                 const TaskResult& result) const {
  return observer->Observe(&TaskObserver::OnContinuationEnd, *this, context, result);
}

}  // namespace ara::task
