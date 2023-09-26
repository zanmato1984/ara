#include "task.h"
#include "task_observer.h"

namespace ara::task {

Status Task::ObserverBegin(ChainedObserver<TaskObserver>* observer,
                           const TaskContext& ctx, TaskId task_id) const {
  return observer->Observe(&TaskObserver::OnTaskBegin, *this, ctx, task_id);
}

Status Task::ObserverEnd(ChainedObserver<TaskObserver>* observer, const TaskContext& ctx,
                         TaskId task_id, const TaskResult& result) const {
  return observer->Observe(&TaskObserver::OnTaskEnd, *this, ctx, task_id, result);
}

Status Continuation::ObserverBegin(ChainedObserver<TaskObserver>* observer,
                                   const TaskContext& ctx) const {
  return observer->Observe(&TaskObserver::OnContinuationBegin, *this, ctx);
}

Status Continuation::ObserverEnd(ChainedObserver<TaskObserver>* observer,
                                 const TaskContext& ctx, const TaskResult& result) const {
  return observer->Observe(&TaskObserver::OnContinuationEnd, *this, ctx, result);
}

}  // namespace ara::task
