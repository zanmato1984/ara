#include "task_group.h"
#include "task_context.h"
#include "task_observer.h"

namespace ara::task {

Status TaskGroup::NotifyFinish(const TaskContext& context) const {
  if (notify_ == nullptr) {
    return Status::OK();
  }

  if (context.task_observer != nullptr) {
    ARA_RETURN_NOT_OK(context.task_observer->Observe(&TaskObserver::OnNotifyFinishBegin,
                                                     *this, context));
  }

  auto status = notify_(context);

  if (context.task_observer != nullptr) {
    ARA_RETURN_NOT_OK(context.task_observer->Observe(&TaskObserver::OnNotifyFinishEnd,
                                                     *this, context, status));
  }

  return status;
}

}  // namespace ara::task
