#include "task_group.h"
#include "task_context.h"
#include "task_observer.h"

namespace ara::task {

Status TaskGroup::NotifyFinish(const TaskContext& ctx) const {
  if (notify_ == nullptr) {
    return Status::OK();
  }

  if (ctx.task_observer != nullptr) {
    ARA_RETURN_NOT_OK(
        ctx.task_observer->Observe(&TaskObserver::OnNotifyFinishBegin, *this, ctx));
  }

  auto status = notify_(ctx);

  if (ctx.task_observer != nullptr) {
    ARA_RETURN_NOT_OK(
        ctx.task_observer->Observe(&TaskObserver::OnNotifyFinishEnd, *this, ctx, status));
  }

  return status;
}

}  // namespace ara::task
